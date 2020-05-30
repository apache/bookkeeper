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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.bookkeeper.util.NativeIO;
import org.apache.bookkeeper.util.ZeroBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple wrapper around FileChannel to add versioning
 * information to the file.
 */
class JournalChannel implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JournalChannel.class);

    final RandomAccessFile randomAccessFile;
    final int fd;
    final FileChannel fc;
    final BufferedChannel bc;
    final int formatVersion;
    long nextPrealloc = 0;

    final byte[] magicWord = "BKLG".getBytes(UTF_8);

    static final int SECTOR_SIZE = 512;
    private static final int START_OF_FILE = -12345;
    private static long cacheDropLagBytes = 8 * 1024 * 1024;

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
    // Adding explicitlac entry
    public static final int V6 = 6;

    static final int HEADER_SIZE = SECTOR_SIZE; // align header to sector size
    static final int VERSION_HEADER_SIZE = 8; // 4byte magic word, 4 byte version
    static final int MIN_COMPAT_JOURNAL_FORMAT_VERSION = V1;
    static final int CURRENT_JOURNAL_FORMAT_VERSION = V6;

    private final long preAllocSize;
    private final int journalAlignSize;
    private final boolean fRemoveFromPageCache;
    public final ByteBuffer zeros;

    // The position of the file channel's last drop position
    private long lastDropPosition = 0L;

    // Mostly used by tests
    JournalChannel(File journalDirectory, long logId) throws IOException {
        this(journalDirectory, logId, 4 * 1024 * 1024, 65536, START_OF_FILE);
    }

    // Open journal for scanning starting from the first record in journal.
    JournalChannel(File journalDirectory, long logId, long preAllocSize, int writeBufferSize) throws IOException {
        this(journalDirectory, logId, preAllocSize, writeBufferSize, START_OF_FILE);
    }

    // Open journal for scanning starting from given position.
    JournalChannel(File journalDirectory, long logId,
                   long preAllocSize, int writeBufferSize, long position) throws IOException {
         this(journalDirectory, logId, preAllocSize, writeBufferSize, SECTOR_SIZE,
                 position, false, V5, Journal.BufferedChannelBuilder.DEFAULT_BCBUILDER);
    }

    // Open journal to write
    JournalChannel(File journalDirectory, long logId,
                   long preAllocSize, int writeBufferSize, int journalAlignSize,
                   boolean fRemoveFromPageCache, int formatVersionToWrite) throws IOException {
        this(journalDirectory, logId, preAllocSize, writeBufferSize, journalAlignSize,
             fRemoveFromPageCache, formatVersionToWrite, Journal.BufferedChannelBuilder.DEFAULT_BCBUILDER);
    }

    JournalChannel(File journalDirectory, long logId,
                   long preAllocSize, int writeBufferSize, int journalAlignSize,
                   boolean fRemoveFromPageCache, int formatVersionToWrite,
                   Journal.BufferedChannelBuilder bcBuilder) throws IOException {
        this(journalDirectory, logId, preAllocSize, writeBufferSize, journalAlignSize,
                START_OF_FILE, fRemoveFromPageCache, formatVersionToWrite, bcBuilder);
    }

    /**
     * Create a journal file.
     * Allows injection of BufferedChannelBuilder for testing purposes.
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
     * @throws IOException
     */
    private JournalChannel(File journalDirectory, long logId,
                           long preAllocSize, int writeBufferSize, int journalAlignSize,
                           long position, boolean fRemoveFromPageCache,
                           int formatVersionToWrite, Journal.BufferedChannelBuilder bcBuilder) throws IOException {
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
            fc = openFileChannel(randomAccessFile);
            formatVersion = formatVersionToWrite;

            int headerSize = (V4 == formatVersion) ? VERSION_HEADER_SIZE : HEADER_SIZE;
            ByteBuffer bb = ByteBuffer.allocate(headerSize);
            ZeroBuffer.put(bb);
            bb.clear();
            bb.put(magicWord);
            bb.putInt(formatVersion);
            bb.clear();
            fc.write(bb);

            bc = bcBuilder.create(fc, writeBufferSize);
            forceWrite(true);
            nextPrealloc = this.preAllocSize;
            fc.write(zeros, nextPrealloc - journalAlignSize);
        } else {  // open an existing file
            randomAccessFile = new RandomAccessFile(fn, "r");
            fc = openFileChannel(randomAccessFile);
            bc = null; // readonly

            ByteBuffer bb = ByteBuffer.allocate(VERSION_HEADER_SIZE);
            int c = fc.read(bb);
            bb.flip();

            if (c == VERSION_HEADER_SIZE) {
                byte[] first4 = new byte[4];
                bb.get(first4);

                if (Arrays.equals(first4, magicWord)) {
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
                throw e;
            }
        }
        if (fRemoveFromPageCache) {
            this.fd = NativeIO.getSysFileDescriptor(randomAccessFile.getFD());
        } else {
            this.fd = -1;
        }
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

    void preAllocIfNeeded(long size) throws IOException {
        if (bc.position() + size > nextPrealloc) {
            nextPrealloc += preAllocSize;
            zeros.clear();
            fc.write(zeros, nextPrealloc - journalAlignSize);
        }
    }

    int read(ByteBuffer dst)
            throws IOException {
        return fc.read(dst);
    }

    @Override
    public void close() throws IOException {
        if (bc != null) {
            bc.close();
        }
    }

    public void forceWrite(boolean forceMetadata) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Journal ForceWrite");
        }
        long newForceWritePosition = bc.forceWrite(forceMetadata);
        //
        // For POSIX_FADV_DONTNEED, we want to drop from the beginning
        // of the file to a position prior to the current position.
        //
        // The cacheDropLagBytes is to prevent dropping a page that will
        // be appended again, which would introduce random seeking on journal
        // device.
        //
        // <======== drop ==========>
        //                           <-----------LAG------------>
        // +------------------------+---------------------------O
        // lastDropPosition     newDropPos             lastForceWritePosition
        //
        if (fRemoveFromPageCache) {
            long newDropPos = newForceWritePosition - cacheDropLagBytes;
            if (lastDropPosition < newDropPos) {
                NativeIO.bestEffortRemoveFromPageCache(fd, lastDropPosition, newDropPos - lastDropPosition);
            }
            this.lastDropPosition = newDropPos;
        }
    }

    @VisibleForTesting
    public static FileChannel openFileChannel(RandomAccessFile randomAccessFile) {
        if (randomAccessFile == null) {
            throw new IllegalArgumentException("Input cannot be null");
        }

        return randomAccessFile.getChannel();
    }
}
