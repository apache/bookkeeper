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
import static org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification.WATCHER_RECYCLER;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.bookkeeper.common.util.Watchable;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the file handle for a ledger's index file that maps entry ids to location.
 * It is used by LedgerCache.
 *
 * <p>
 * Ledger index file is made of a header and several fixed-length index pages, which records the offsets of data stored
 * in entry loggers
 * <pre>&lt;header&gt;&lt;index pages&gt;</pre>
 * <b>Header</b> is formated as below:
 * <pre>&lt;magic bytes&gt;&lt;len of master key&gt;&lt;master key&gt;</pre>
 * <ul>
 * <li>magic bytes: 4 bytes, 'BKLE', version: 4 bytes
 * <li>len of master key: indicates length of master key. -1 means no master key stored in header.
 * <li>master key: master key
 * <li>state: bit map to indicate the state, 32 bits.
 * </ul>
 * <b>Index page</b> is a fixed-length page, which contains serveral entries which point to the offsets of data stored
 * in entry loggers.
 * </p>
 */
class FileInfo extends Watchable<LastAddConfirmedUpdateNotification> {
    private static final Logger LOG = LoggerFactory.getLogger(FileInfo.class);

    static final int NO_MASTER_KEY = -1;
    static final int STATE_FENCED_BIT = 0x1;

    private FileChannel fc;
    private File lf;
    private ByteBuffer explicitLac = null;

    byte[] masterKey;

    /**
     * The fingerprint of a ledger index file.
     */
    public static final int SIGNATURE = ByteBuffer.wrap("BKLE".getBytes(UTF_8)).getInt();

    // No explicitLac
    static final int V0 = 0;
    // Adding explicitLac
    static final int V1 = 1;
    // current version of FileInfo header is V1
    public static final int CURRENT_HEADER_VERSION = V1;

    static final long START_OF_DATA = 1024;
    private long size;
    private boolean isClosed;
    private long sizeSinceLastwrite;

    // bit map for states of the ledger.
    private int stateBits;
    private boolean needFlushHeader = false;

    // lac
    private Long lac = null;

    // file access mode
    protected String mode;

    // this FileInfo Header Version
    int headerVersion;

    private boolean deleted;

    public FileInfo(File lf, byte[] masterKey, int fileInfoVersionToWrite) throws IOException {
        super(WATCHER_RECYCLER);

        this.lf = lf;
        this.masterKey = masterKey;
        mode = "rw";
        this.headerVersion = fileInfoVersionToWrite;
        this.deleted = false;
    }

    synchronized Long getLastAddConfirmed() {
        return lac;
    }

    long setLastAddConfirmed(long lac) {
        long lacToReturn;
        boolean changed = false;
        synchronized (this) {
            if (null == this.lac || this.lac < lac) {
                this.lac = lac;
                changed = true;
            }
            lacToReturn = this.lac;
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Updating LAC {} , {}", lacToReturn, lac);
        }

        if (changed) {
            notifyWatchers(LastAddConfirmedUpdateNotification.FUNC, lacToReturn);
        }
        return lacToReturn;
    }

    synchronized boolean waitForLastAddConfirmedUpdate(long previousLAC,
                                                       Watcher<LastAddConfirmedUpdateNotification> watcher) {
        if ((null != lac && lac > previousLAC) || isClosed) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Wait For LAC {} , {}", this.lac, previousLAC);
            }
            return false;
        }

        addWatcher(watcher);
        return true;
    }

    synchronized void cancelWaitForLastAddConfirmedUpdate(Watcher<LastAddConfirmedUpdateNotification> watcher) {
        deleteWatcher(watcher);
    }

    public boolean isClosed() {
        return isClosed;
    }

    public synchronized File getLf() {
        return lf;
    }

    public long getSizeSinceLastwrite() {
        return sizeSinceLastwrite;
    }

    public ByteBuf getExplicitLac() {
        ByteBuf retLac = null;
        synchronized (this) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("fileInfo:GetLac: {}", explicitLac);
            }
            if (explicitLac != null) {
                retLac = Unpooled.buffer(explicitLac.capacity());
                explicitLac.rewind(); //copy from the beginning
                retLac.writeBytes(explicitLac);
                explicitLac.rewind();
                return retLac;
            }
        }
        return retLac;
    }

    public void setExplicitLac(ByteBuf lac) {
        long explicitLacValue;
        synchronized (this) {
            if (explicitLac == null) {
                explicitLac = ByteBuffer.allocate(lac.capacity());
            }
            lac.readBytes(explicitLac);
            explicitLac.rewind();

            // skip the ledger id
            explicitLac.getLong();
            explicitLacValue = explicitLac.getLong();
            explicitLac.rewind();
            if (LOG.isDebugEnabled()) {
                LOG.debug("fileInfo:SetLac: {}", explicitLac);
            }
            needFlushHeader = true;
        }
        setLastAddConfirmed(explicitLacValue);
    }

    public synchronized void readHeader() throws IOException {
        if (lf.exists()) {
            if (fc != null) {
                return;
            }

            fc = new RandomAccessFile(lf, mode).getChannel();
            size = fc.size();
            sizeSinceLastwrite = size;

            // avoid hang on reading partial index
            ByteBuffer bb = ByteBuffer.allocate((int) (Math.min(size, START_OF_DATA)));
            while (bb.hasRemaining()) {
                fc.read(bb);
            }
            bb.flip();
            if (bb.getInt() != SIGNATURE) {
                throw new IOException("Missing ledger signature while reading header for " + lf);
            }
            int version = bb.getInt();
            if (version > CURRENT_HEADER_VERSION) {
                throw new IOException("Incompatible ledger version " + version + " while reading header for " + lf);
            }
            this.headerVersion = version;

            int length = bb.getInt();
            if (length < 0) {
                throw new IOException("Length " + length + " is invalid while reading header for " + lf);
            } else if (length > bb.remaining()) {
                throw new BufferUnderflowException();
            }
            masterKey = new byte[length];
            bb.get(masterKey);
            stateBits = bb.getInt();

            if (this.headerVersion >= V1) {
                int explicitLacBufLength = bb.getInt();
                if (explicitLacBufLength == 0) {
                    explicitLac = null;
                } else if (explicitLacBufLength >= DigestManager.LAC_METADATA_LENGTH) {
                    if (explicitLac == null) {
                        explicitLac = ByteBuffer.allocate(explicitLacBufLength);
                    }
                    byte[] explicitLacBufArray = new byte[explicitLacBufLength];
                    bb.get(explicitLacBufArray);
                    explicitLac.put(explicitLacBufArray);
                    explicitLac.rewind();
                } else {
                    throw new IOException("ExplicitLacBufLength " + explicitLacBufLength
                            + " is invalid while reading header for " + lf);
                }
            }

            needFlushHeader = false;
        } else {
            throw new IOException("Ledger index file " + lf + " does not exist");
        }
    }

    public synchronized boolean isDeleted() {
        return deleted;
    }

    public static class FileInfoDeletedException extends IOException {
        FileInfoDeletedException() {
            super("FileInfo already deleted");
        }
    }

    @VisibleForTesting
    void checkOpen(boolean create) throws IOException {
        checkOpen(create, false);
    }

    private synchronized void checkOpen(boolean create, boolean openBeforeClose)
            throws IOException {
        if (deleted) {
            throw new FileInfoDeletedException();
        }
        if (fc != null) {
            return;
        }
        boolean exists = lf.exists();
        if (masterKey == null && !exists) {
            throw new IOException(lf + " not found");
        }

        if (!exists) {
            if (create) {
                // delayed the creation of parents directories
                checkParents(lf);
                fc = new RandomAccessFile(lf, mode).getChannel();
                size = fc.size();
                if (size == 0) {
                    writeHeader();
                }
            }
        } else {
            if (openBeforeClose) {
                // if it is checking for close, skip reading header
                return;
            }
            try {
                readHeader();
            } catch (BufferUnderflowException buf) {
                LOG.warn("Exception when reading header of {}.", lf, buf);
                if (null != masterKey) {
                    LOG.warn("Attempting to write header of {} again.", lf);
                    writeHeader();
                } else {
                    throw new IOException("Error reading header " + lf);
                }
            }
        }
    }

    private void writeHeader() throws IOException {
        ByteBuffer bb = ByteBuffer.allocate((int) START_OF_DATA);
        bb.putInt(SIGNATURE);
        bb.putInt(this.headerVersion);
        bb.putInt(masterKey.length);
        bb.put(masterKey);
        bb.putInt(stateBits);
        if (this.headerVersion >= V1) {
            if (explicitLac != null) {
                explicitLac.rewind();
                bb.putInt(explicitLac.capacity());
                bb.put(explicitLac);
                explicitLac.rewind();
            } else {
                bb.putInt(0);
            }
        }
        bb.rewind();
        fc.position(0);
        fc.write(bb);
    }

    public synchronized boolean isFenced() throws IOException {
        checkOpen(false);
        return (stateBits & STATE_FENCED_BIT) == STATE_FENCED_BIT;
    }

    /**
     * @return true if set fence succeed, otherwise false when
     * it already fenced or failed to set fenced.
     */
    public boolean setFenced() throws IOException {
        boolean returnVal = false;
        boolean changed = false;
        synchronized (this) {
            checkOpen(false);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Try to set fenced state in file info {} : state bits {}.", lf, stateBits);
            }
            if ((stateBits & STATE_FENCED_BIT) != STATE_FENCED_BIT) {
                // not fenced yet
                stateBits |= STATE_FENCED_BIT;
                needFlushHeader = true;
                synchronized (this) {
                    changed = true;
                }
                returnVal = true;
            }
        }
        if (changed) {
            notifyWatchers(LastAddConfirmedUpdateNotification.FUNC, Long.MAX_VALUE);
        }
        return returnVal;
    }

    // flush the header when header is changed
    public synchronized void flushHeader() throws IOException {
        if (needFlushHeader) {
            checkOpen(true);
            writeHeader();
            needFlushHeader = false;
        }
    }

    public synchronized long size() throws IOException {
        checkOpen(false);
        long rc = size - START_OF_DATA;
        if (rc < 0) {
            rc = 0;
        }
        return rc;
    }

    public int read(ByteBuffer bb, long position, boolean bestEffort)
            throws IOException {
        return readAbsolute(bb, position + START_OF_DATA, bestEffort);
    }

    /**
     * Read data from position <i>start</i> to fill the byte buffer <i>bb</i>.
     * If <i>bestEffort </i> is provided, it would return when it reaches EOF.
     * Otherwise, it would throw {@link org.apache.bookkeeper.bookie.ShortReadException}
     * if it reaches EOF.
     *
     * @param bb
     *          byte buffer of data
     * @param start
     *          start position to read data
     * @param bestEffort
     *          flag indicates if it is a best-effort read
     * @return number of bytes read
     * @throws IOException
     */
    private int readAbsolute(ByteBuffer bb, long start, boolean bestEffort)
            throws IOException {
        checkOpen(false);
        synchronized (this) {
            if (fc == null) {
                return 0;
            }
        }
        int total = 0;
        int rc = 0;
        while (bb.remaining() > 0) {
            synchronized (this) {
                rc = fc.read(bb, start);
            }
            if (rc <= 0) {
                if (bestEffort) {
                    return total;
                } else {
                    throw new ShortReadException("Short read at " + getLf().getPath() + "@" + start);
                }
            }
            total += rc;
            // should move read position
            start += rc;
        }
        return total;
    }

    /**
     * Close a file info. Generally, force should be set to true. If set to false metadata will not be flushed and
     * accessing metadata before restart and recovery will be unsafe (since reloading from the index file will
     * cause metadata to be lost). Setting force=false helps avoid expensive file create during shutdown with many
     * dirty ledgers, and is safe because ledger metadata will be recovered before being accessed again.
     *
     * @param force
     *          if set to true, the index is forced to create before closed,
     *          if set to false, the index is not forced to create.
     */
    public void close(boolean force) throws IOException {
        boolean changed = false;
        synchronized (this) {
            if (isClosed) {
                return;
            }
            isClosed = true;
            checkOpen(force, true);
            // Any time when we force close a file, we should try to flush header.
            // otherwise, we might lose fence bit.
            if (force) {
                flushHeader();
            }
            changed = true;
            if (fc != null) {
                fc.close();
            }
            fc = null;
        }
        if (changed) {
            notifyWatchers(LastAddConfirmedUpdateNotification.FUNC, Long.MAX_VALUE);
        }
    }

    public synchronized long write(ByteBuffer[] buffs, long position) throws IOException {
        checkOpen(true);
        long total = 0;
        try {
            fc.position(position + START_OF_DATA);
            while (buffs[buffs.length - 1].remaining() > 0) {
                long rc = fc.write(buffs);
                if (rc <= 0) {
                    throw new IOException("Short write");
                }
                total += rc;
            }
        } finally {
            fc.force(true);
            long newsize = position + START_OF_DATA + total;
            if (newsize > size) {
                size = newsize;
            }
        }
        sizeSinceLastwrite = fc.size();
        return total;
    }

    /**
     * Copies current file contents upto specified size to the target file and
     * deletes the current file. If size not known then pass size as
     * Long.MAX_VALUE to copy complete file.
     */
    public synchronized void moveToNewLocation(File newFile, long size) throws IOException {
        checkOpen(false);
        // If the channel is null, or same file path, just return.
        if (null == fc || isSameFile(newFile)) {
            return;
        }
        if (size > fc.size()) {
            size = fc.size();
        }
        File rlocFile = new File(newFile.getParentFile(), newFile.getName() + IndexPersistenceMgr.RLOC);
        if (!rlocFile.exists()) {
            checkParents(rlocFile);
            if (!rlocFile.createNewFile()) {
                throw new IOException("Creating new cache index file " + rlocFile + " failed ");
            }
        }
        // copy contents from old.idx to new.idx.rloc
        FileChannel newFc = new RandomAccessFile(rlocFile, "rw").getChannel();
        try {
            long written = 0;
            while (written < size) {
                long count = fc.transferTo(written, size, newFc);
                if (count <= 0) {
                    throw new IOException("Copying to new location " + rlocFile + " failed");
                }
                written += count;
            }
            if (written <= 0 && size > 0) {
                throw new IOException("Copying to new location " + rlocFile + " failed");
            }
        } finally {
            newFc.force(true);
            newFc.close();
        }
        // delete old.idx
        fc.close();
        if (!delete()) {
            LOG.error("Failed to delete the previous index file " + lf);
            throw new IOException("Failed to delete the previous index file " + lf);
        }

        // rename new.idx.rloc to new.idx
        if (!rlocFile.renameTo(newFile)) {
            LOG.error("Failed to rename " + rlocFile + " to " + newFile);
            throw new IOException("Failed to rename " + rlocFile + " to " + newFile);
        }
        fc = new RandomAccessFile(newFile, mode).getChannel();
        lf = newFile;
    }

    public synchronized byte[] getMasterKey() throws IOException {
        checkOpen(false);
        return masterKey;
    }

    public synchronized boolean delete() {
        deleted = true;
        return lf.delete();
    }

    private static void checkParents(File f) throws IOException {
        File parent = f.getParentFile();
        if (parent.exists()) {
            return;
        }
        if (!parent.mkdirs()) {
            throw new IOException("Counldn't mkdirs for " + parent);
        }
    }

    public synchronized boolean isSameFile(File f) {
        return this.lf.equals(f);
    }
}
