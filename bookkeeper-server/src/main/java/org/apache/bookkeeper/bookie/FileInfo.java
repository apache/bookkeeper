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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.nio.channels.FileChannel;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the file handle for a ledger's index file that maps entry ids to location.
 * It is used by LedgerCache.
 *
 * <p>
 * Ledger index file is made of a header and several fixed-length index pages, which records the offsets of data stored in entry loggers
 * <pre>&lt;header&gt;&lt;index pages&gt;</pre>
 * <b>Header</b> is formated as below:
 * <pre>&lt;magic bytes&gt;&lt;len of master key&gt;&lt;master key&gt;</pre>
 * <ul>
 * <li>magic bytes: 4 bytes, 'BKLE', version: 4 bytes
 * <li>len of master key: indicates length of master key. -1 means no master key stored in header.
 * <li>master key: master key
 * <li>state: bit map to indicate the state, 32 bits.
 * </ul>
 * <b>Index page</b> is a fixed-length page, which contains serveral entries which point to the offsets of data stored in entry loggers.
 * </p>
 */
class FileInfo {
    static Logger LOG = LoggerFactory.getLogger(FileInfo.class);

    static final int NO_MASTER_KEY = -1;
    static final int STATE_FENCED_BIT = 0x1;

    private FileChannel fc;
    private File lf;
    byte[] masterKey;

    /**
     * The fingerprint of a ledger index file
     */
    static final public int signature = ByteBuffer.wrap("BKLE".getBytes()).getInt();
    static final public int headerVersion = 0;

    static final long START_OF_DATA = 1024;
    private long size;
    private int useCount;
    private boolean isClosed;
    private long sizeSinceLastwrite;

    // bit map for states of the ledger.
    private int stateBits;
    private boolean needFlushHeader = false;

    // file access mode
    protected String mode;

    public FileInfo(File lf, byte[] masterKey) throws IOException {
        this.lf = lf;

        this.masterKey = masterKey;
        mode = "rw";
    }

    public File getLf() {
        return lf;
    }

    public long getSizeSinceLastwrite() {
        return sizeSinceLastwrite;
    }

    synchronized public void readHeader() throws IOException {
        if (lf.exists()) {
            if (fc != null) {
                return;
            }

            fc = new RandomAccessFile(lf, mode).getChannel();
            size = fc.size();
            sizeSinceLastwrite = size;

            // avoid hang on reading partial index
            ByteBuffer bb = ByteBuffer.allocate((int)(Math.min(size, START_OF_DATA)));
            while(bb.hasRemaining()) {
                fc.read(bb);
            }
            bb.flip();
            if (bb.getInt() != signature) {
                throw new IOException("Missing ledger signature");
            }
            int version = bb.getInt();
            if (version != headerVersion) {
                throw new IOException("Incompatible ledger version " + version);
            }
            int length = bb.getInt();
            if (length < 0) {
                throw new IOException("Length " + length + " is invalid");
            } else if (length > bb.remaining()) {
                throw new BufferUnderflowException();
            }
            masterKey = new byte[length];
            bb.get(masterKey);
            stateBits = bb.getInt();
            needFlushHeader = false;
        } else {
            throw new IOException("Ledger index file does not exist");
        }
    }

    synchronized private void checkOpen(boolean create) throws IOException {
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
            try {
                readHeader();
            } catch (BufferUnderflowException buf) {
                LOG.warn("Exception when reading header of {} : {}", lf, buf);
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
        ByteBuffer bb = ByteBuffer.allocate((int)START_OF_DATA);
        bb.putInt(signature);
        bb.putInt(headerVersion);
        bb.putInt(masterKey.length);
        bb.put(masterKey);
        bb.putInt(stateBits);
        bb.rewind();
        fc.position(0);
        fc.write(bb);
    }

    synchronized public boolean isFenced() throws IOException {
        checkOpen(false);
        return (stateBits & STATE_FENCED_BIT) == STATE_FENCED_BIT;
    }

    /**
     * @return true if set fence succeed, otherwise false when
     * it already fenced or failed to set fenced.
     */
    synchronized public boolean setFenced() throws IOException {
        checkOpen(false);
        LOG.debug("Try to set fenced state in file info {} : state bits {}.", lf, stateBits);
        if ((stateBits & STATE_FENCED_BIT) != STATE_FENCED_BIT) {
            // not fenced yet
            stateBits |= STATE_FENCED_BIT;
            needFlushHeader = true;
            return true;
        } else {
            return false;
        }
    }

    // flush the header when header is changed
    synchronized public void flushHeader() throws IOException {
        if (needFlushHeader) {
            checkOpen(true);
            writeHeader();
            needFlushHeader = false;
        }
    }

    synchronized public long size() throws IOException {
        checkOpen(false);
        long rc = size-START_OF_DATA;
        if (rc < 0) {
            rc = 0;
        }
        return rc;
    }

    synchronized public int read(ByteBuffer bb, long position) throws IOException {
        return readAbsolute(bb, position + START_OF_DATA);
    }

    private int readAbsolute(ByteBuffer bb, long start) throws IOException {
        checkOpen(false);
        if (fc == null) {
            return 0;
        }
        int total = 0;
        while(bb.remaining() > 0) {
            int rc = fc.read(bb, start);
            if (rc <= 0) {
                throw new IOException("Short read");
            }
            total += rc;
            // should move read position
            start += rc;
        }
        return total;
    }

    /**
     * Close a file info
     *
     * @param force
     *          if set to true, the index is forced to create before closed,
     *          if set to false, the index is not forced to create.
     */
    synchronized public void close(boolean force) throws IOException {
        isClosed = true;
        checkOpen(force);
        if (useCount == 0 && fc != null) {
            fc.close();
        }
    }

    synchronized public long write(ByteBuffer[] buffs, long position) throws IOException {
        checkOpen(true);
        long total = 0;
        try {
            fc.position(position+START_OF_DATA);
            while(buffs[buffs.length-1].remaining() > 0) {
                long rc = fc.write(buffs);
                if (rc <= 0) {
                    throw new IOException("Short write");
                }
                total += rc;
            }
        } finally {
            fc.force(true);
            long newsize = position+START_OF_DATA+total;
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
        if (fc != null) {
            if (size > fc.size()) {
                size = fc.size();
            }
            File rlocFile = new File(newFile.getParentFile(), newFile.getName() + LedgerCacheImpl.RLOC);
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
        }
        lf = newFile;
    }

    synchronized public byte[] getMasterKey() throws IOException {
        checkOpen(false);
        return masterKey;
    }

    synchronized public void use() {
        useCount++;
    }

    @VisibleForTesting
    synchronized int getUseCount() {
        return useCount;
    }

    synchronized public void release() {
        useCount--;
        if (isClosed && useCount == 0 && fc != null) {
            try {
                fc.close();
            } catch (IOException e) {
                LOG.error("Error closing file channel", e);
            }
        }
    }

    public boolean delete() {
        return lf.delete();
    }

    static final private void checkParents(File f) throws IOException {
        File parent = f.getParentFile();
        if (parent.exists()) {
            return;
        }
        if (parent.mkdirs() == false) {
            throw new IOException("Counldn't mkdirs for " + parent);
        }
    }
}
