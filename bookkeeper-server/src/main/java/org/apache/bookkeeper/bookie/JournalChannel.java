/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.bookie;

import java.io.Closeable;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.NativeIO;
import org.apache.bookkeeper.util.ZeroBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.*;
import static org.apache.bookkeeper.util.NativeIO.*;

/**
 * Simple wrapper around FileChannel to add versioning
 * information to the file.
 */
class JournalChannel implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(JournalChannel.class);

    final RandomAccessFile randomAccessFile;
    final int fd;
    final FileChannel fc;
    final BufferedChannel bc;
    final int formatVersion;
    long nextPrealloc = 0;

    final byte[] MAGIC_WORD = "BKLG".getBytes(UTF_8);

    final static int SECTOR_SIZE = 512;
    private final static int START_OF_FILE = -12345;
    private static long CACHE_DROP_LAG_BYTES = 8 * 1024 * 1024;

    // No header
    static final int V1 = 1;
    // Adding header
    static final int V2 = 2;
    // Adding ledger key
    static final int V3 = 3;
    // Adding fencing key
    static final int V4 = 4;
    // 1) expanding header to 512
    // 2) Padding writes to align sector size
    static final int V5 = 5;

    static final int HEADER_SIZE = SECTOR_SIZE; // align header to sector size
    static final int VERSION_HEADER_SIZE = 8; // 4byte magic word, 4 byte version
    static final int MIN_COMPAT_JOURNAL_FORMAT_VERSION = V1;
    static final int CURRENT_JOURNAL_FORMAT_VERSION = V5;

    private final long preAllocSize;
    private final int journalAlignSize;
    private final boolean fRemoveFromPageCache;
    public final ByteBuffer zeros;

    // The position of the file channel's last drop position
    private long lastDropPosition = 0L;

    // Stats
    private final OpStatsLogger journalPreallocationStats;
    private final Counter journalForceWriteCounter;
    private final OpStatsLogger journalForceWriteStats;

    // Mostly used by tests
    JournalChannel(File journalDirectory, long logId) throws IOException {
        this(journalDirectory, logId, 4 * 1024 * 1024, 65536, START_OF_FILE, NullStatsLogger.INSTANCE);
    }

    // Open journal for scanning starting from the first record in journal.
    JournalChannel(File journalDirectory, long logId, long preAllocSize, int writeBufferSize, StatsLogger statsLogger)
            throws IOException {
        this(journalDirectory, logId, preAllocSize, writeBufferSize, START_OF_FILE, statsLogger);
    }

    // Open journal for scanning starting from given position.
    JournalChannel(File journalDirectory, long logId,
                   long preAllocSize, int writeBufferSize, long position, StatsLogger statsLogger)
            throws IOException {
         this(journalDirectory, logId, preAllocSize, writeBufferSize, SECTOR_SIZE, position, false, V5, statsLogger);
    }

    // Open journal to write
    JournalChannel(File journalDirectory, long logId,
                   long preAllocSize, int writeBufferSize, int journalAlignSize,
                   boolean fRemoveFromPageCache, int formatVersionToWrite,
                   StatsLogger statsLogger) throws IOException {
        this(journalDirectory, logId, preAllocSize, writeBufferSize, journalAlignSize,
             START_OF_FILE, fRemoveFromPageCache, formatVersionToWrite, statsLogger);
    }

    /**
     * Create a journal file.
     *
     * @param journalDirectory
     *          directory to store the journal file.
     * @param logId
     *          log id for the journal file.
     * @param preAllocSize
     *          pre allocation size.
     * @param writeBufferSize
     *          write buffer size.
     * @param journalAlignSize
     *          size to align journal writes.
     * @param position
     *          position to start read/write
     * @param fRemoveFromPageCache
     *          whether to remove cached pages from page cache.
     * @param formatVersionToWrite
     *          format version to write
     * @param statsLogger
     *          stats logger to record stats
     * @throws IOException
     */
    private JournalChannel(File journalDirectory,
                           long logId,
                           long preAllocSize,
                           int writeBufferSize,
                           int journalAlignSize,
                           long position,
                           boolean fRemoveFromPageCache,
                           int formatVersionToWrite,
                           StatsLogger statsLogger)
            throws IOException {
        this.journalAlignSize = journalAlignSize;
        this.zeros = ByteBuffer.allocate(journalAlignSize);
        this.preAllocSize = preAllocSize - preAllocSize % journalAlignSize;
        this.fRemoveFromPageCache = fRemoveFromPageCache;
        File fn = new File(journalDirectory, Long.toHexString(logId) + ".txn");

        if (formatVersionToWrite < V4) {
            throw new IOException("Invalid journal format to write : version = " + formatVersionToWrite);
        }

        LOG.info("Opening journal {}", fn);
        if (!fn.exists()) { // new file, write version
            if (!fn.createNewFile()) {
                LOG.error("Journal file {}, that shouldn't exist, already exists. "
                          + " is there another bookie process running?", fn);
                throw new IOException("File " + fn
                        + " suddenly appeared, is another bookie process running?");
            }
            randomAccessFile = new RandomAccessFile(fn, "rw");
            fd = NativeIO.getSysFileDescriptor(randomAccessFile.getFD());
            fc = randomAccessFile.getChannel();
            formatVersion = formatVersionToWrite;

            // preallocate the space the header
            preallocate();

            int headerSize = (V4 == formatVersion) ? VERSION_HEADER_SIZE : HEADER_SIZE;
            ByteBuffer bb = ByteBuffer.allocate(headerSize);
            ZeroBuffer.put(bb);
            bb.clear();
            bb.put(MAGIC_WORD);
            bb.putInt(formatVersion);
            bb.clear();
            fc.write(bb);

            bc = new BufferedChannel(fc, writeBufferSize);

            // sync the file
            // syncRangeOrForceWrite(0, HEADER_SIZE);
        } else {  // open an existing file
            randomAccessFile = new RandomAccessFile(fn, "r");
            fd = NativeIO.getSysFileDescriptor(randomAccessFile.getFD());
            fc = randomAccessFile.getChannel();
            bc = null; // readonly

            ByteBuffer bb = ByteBuffer.allocate(VERSION_HEADER_SIZE);
            int c = fc.read(bb);
            bb.flip();

            if (c == VERSION_HEADER_SIZE) {
                byte[] first4 = new byte[4];
                bb.get(first4);

                if (Arrays.equals(first4, MAGIC_WORD)) {
                    formatVersion = bb.getInt();
                } else {
                    // pre magic word journal, reset to 0;
                    formatVersion = V1;
                }
            } else {
                // no header, must be old version
                formatVersion = V1;
            }

            if (formatVersion < MIN_COMPAT_JOURNAL_FORMAT_VERSION
                || formatVersion > CURRENT_JOURNAL_FORMAT_VERSION) {
                String err = String.format("Invalid journal version, unable to read."
                        + " Expected between (%d) and (%d), got (%d)",
                        MIN_COMPAT_JOURNAL_FORMAT_VERSION, CURRENT_JOURNAL_FORMAT_VERSION,
                        formatVersion);
                LOG.error(err);
                throw new IOException(err);
            }

            try {
                if (position == START_OF_FILE) {
                    if (formatVersion >= V5) {
                        fc.position(HEADER_SIZE);
                    } else if (formatVersion >= V2) {
                        fc.position(VERSION_HEADER_SIZE);
                    } else {
                        fc.position(0);
                    }
                } else {
                    fc.position(position);
                }
            } catch (IOException e) {
                LOG.error("Bookie journal file can seek to position :", e);
            }
        }

        // Stats
        this.journalForceWriteCounter = statsLogger.getCounter(JOURNAL_NUM_FORCE_WRITES);
        this.journalForceWriteStats = statsLogger.getOpStatsLogger(JOURNAL_FORCE_WRITE_LATENCY);
        this.journalPreallocationStats = statsLogger.getOpStatsLogger(JOURNAL_PREALLOCATION);

        LOG.info("Opened journal {} : fd {}", fn, fd);
    }

    int getFormatVersion() {
        return formatVersion;
    }

    BufferedChannel getBufferedChannel() throws IOException {
        if (bc == null) {
            throw new IOException("Read only journal channel");
        }
        return bc;
    }

    private void preallocate() throws IOException {
        long prevPrealloc = nextPrealloc;
        nextPrealloc = prevPrealloc + preAllocSize;
        if (!NativeIO.fallocateIfPossible(fd, prevPrealloc, preAllocSize)) {
            zeros.clear();
            fc.write(zeros, nextPrealloc - journalAlignSize);
        }
    }

    void preAllocIfNeeded(long size) throws IOException {
        preAllocIfNeeded(size, null);
    }

    void preAllocIfNeeded(long size, Stopwatch stopwatch) throws IOException {
        if (bc.position() + size > nextPrealloc) {
            if (null != stopwatch) {
                stopwatch.reset().start();
            }
            preallocate();
            if (null != stopwatch) {
                journalPreallocationStats.registerSuccessfulEvent(
                        stopwatch.stop().elapsedTime(TimeUnit.MICROSECONDS),
                        TimeUnit.MICROSECONDS);
            }
        }
    }

    int read(ByteBuffer dst)
            throws IOException {
        return fc.read(dst);
    }

    public void close() throws IOException {
        fc.close();
    }

    public void startSyncRange(long offset, long bytes) throws IOException {
        NativeIO.syncFileRangeIfPossible(fd, offset, bytes, SYNC_FILE_RANGE_WRITE);
    }

    public boolean syncRangeIfPossible(long offset, long bytes) throws IOException {
        if (NativeIO.syncFileRangeIfPossible(fd, offset, bytes,
                SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER)) {
            removeFromPageCacheIfPossible(offset + bytes);
            return false;
        } else {
            return false;
        }
    }

    public void forceWrite(boolean forceMetadata) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Journal ForceWrite");
        }
        long startTimeNanos = MathUtils.nowInNano();
        forceWriteImpl(forceMetadata);
        // collect stats
        journalForceWriteCounter.inc();
        journalForceWriteStats.registerSuccessfulEvent(
                MathUtils.elapsedMicroSec(startTimeNanos), TimeUnit.MICROSECONDS);
    }

    private void removeFromPageCacheIfPossible(long offset) {
        //
        // For POSIX_FADV_DONTNEED, we want to drop from the beginning
        // of the file to a position prior to the current position.
        //
        // The CACHE_DROP_LAG_BYTES is to prevent dropping a page that will
        // be appended again, which would introduce random seeking on journal
        // device.
        //
        // <======== drop ==========>
        //                           <-----------LAG------------>
        // +------------------------+---------------------------O
        // lastDropPosition     newDropPos             lastForceWritePosition
        //
        if (fRemoveFromPageCache) {
            long newDropPos = offset - CACHE_DROP_LAG_BYTES;
            if (lastDropPosition < newDropPos) {
                NativeIO.bestEffortRemoveFromPageCache(fd, lastDropPosition, newDropPos - lastDropPosition);
            }
            this.lastDropPosition = newDropPos;
        }
    }

    private void forceWriteImpl(boolean forceMetadata) throws IOException {
        long newForceWritePosition = bc.forceWrite(forceMetadata);
        removeFromPageCacheIfPossible(newForceWritePosition);
    }

    public void syncRangeOrForceWrite(long offset, long bytes) throws IOException {
        long startTimeNanos = MathUtils.nowInNano();
        if (!syncRangeIfPossible(offset, bytes)) {
            forceWriteImpl(false);
        }
        // collect stats
        journalForceWriteCounter.inc();
        journalForceWriteStats.registerSuccessfulEvent(
                MathUtils.elapsedMicroSec(startTimeNanos),
                TimeUnit.MICROSECONDS);
    }
}
