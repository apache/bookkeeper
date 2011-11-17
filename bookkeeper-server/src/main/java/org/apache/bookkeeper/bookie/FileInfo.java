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
import java.nio.channels.FileChannel;

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
 * <li>magic bytes: 8 bytes, 'BKLE\0\0\0\0'
 * <li>len of master key: indicates length of master key. -1 means no master key stored in header.
 * <li>master key: master key
 * </ul>
 * <b>Index page</b> is a fixed-length page, which contains serveral entries which point to the offsets of data stored in entry loggers.
 * </p>
 */
class FileInfo {
    static Logger LOG = LoggerFactory.getLogger(FileInfo.class);

    static final int NO_MASTER_KEY = -1;

    private FileChannel fc;
    private final File lf;
    /**
     * The fingerprint of a ledger index file
     */
    private byte header[] = "BKLE\0\0\0\0".getBytes();
    static final long START_OF_DATA = 1024;
    private long size;
    private int useCount;
    private boolean isClosed;
    public FileInfo(File lf) throws IOException {
        this.lf = lf;
        fc = new RandomAccessFile(lf, "rws").getChannel();
        size = fc.size();
        if (size == 0) {
            fc.write(ByteBuffer.wrap(header));
            // write NO_MASTER_KEY, which means there is no master key
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putInt(NO_MASTER_KEY);
            buf.flip();
            fc.write(buf);
        }
    }

    /**
     * Write master key to index file header
     *
     * @param masterKey master key to store
     * @return void
     * @throws IOException
     */
    synchronized public void writeMasterKey(byte[] masterKey) throws IOException {
        // write master key
        if (masterKey == null ||
            masterKey.length + 4 + header.length > START_OF_DATA) {
            throw new IOException("master key is more than " + (START_OF_DATA - 4 - header.length));
        }

        int len = masterKey.length;
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        lenBuf.putInt(len);
        lenBuf.flip();
        fc.position(header.length);
        fc.write(lenBuf);
        fc.write(ByteBuffer.wrap(masterKey));
    }

    /**
     * Read master key
     *
     * @return master key. null means no master key stored in index header
     * @throws IOException
     */
    synchronized public byte[] readMasterKey() throws IOException {
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        int total = readAbsolute(lenBuf, header.length);
        if (total != 4) {
            throw new IOException("Short read during reading master key length");
        }
        lenBuf.rewind();
        int len = lenBuf.getInt();
        if (len == NO_MASTER_KEY) {
            return null;
        }

        byte[] masterKey = new byte[len];
        total = readAbsolute(ByteBuffer.wrap(masterKey), header.length + 4);
        if (total != len) {
            throw new IOException("Short read during reading master key");
        }
        return masterKey;
    }

    synchronized public long size() {
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

    synchronized public void close() throws IOException {
        isClosed = true;
        if (useCount == 0) {
            fc.close();
        }
    }

    synchronized public long write(ByteBuffer[] buffs, long position) throws IOException {
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
            long newsize = position+START_OF_DATA+total;
            if (newsize > size) {
                size = newsize;
            }
        }
        return total;
    }

    synchronized public void use() {
        useCount++;
    }

    synchronized public void release() {
        useCount--;
        if (isClosed && useCount == 0) {
            try {
                fc.close();
            } catch (IOException e) {
                LOG.error("Error closing file channel", e);
            }
        }
    }

    /**
     * Getter to a handle on the actual ledger index file.
     * This is used when we are deleting a ledger and want to physically remove the index file.
     */
    File getFile() {
        return lf;
    }

}
