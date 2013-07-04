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

import java.util.Arrays;

import java.io.Closeable;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple wrapper around FileChannel to add versioning
 * information to the file.
 */
class JournalChannel implements Closeable {
    static Logger LOG = LoggerFactory.getLogger(JournalChannel.class);

    final FileChannel fc;
    final BufferedChannel bc;
    final int formatVersion;
    long nextPrealloc = 0;

    final byte[] MAGIC_WORD = "BKLG".getBytes();

    private final static int START_OF_FILE = -12345;

    int HEADER_SIZE = 8; // 4byte magic word, 4 byte version
    int MIN_COMPAT_JOURNAL_FORMAT_VERSION = 1;
    int CURRENT_JOURNAL_FORMAT_VERSION = 4;

    public final static long preAllocSize = 4*1024*1024;
    public final static ByteBuffer zeros = ByteBuffer.allocate(512);

    JournalChannel(File journalDirectory, long logId) throws IOException {
        this(journalDirectory, logId, START_OF_FILE);
    }

    JournalChannel(File journalDirectory, long logId, long position) throws IOException {
        File fn = new File(journalDirectory, Long.toHexString(logId) + ".txn");

        LOG.info("Opening journal {}", fn);
        if (!fn.exists()) { // new file, write version
            if (!fn.createNewFile()) {
                LOG.error("Journal file {}, that shouldn't exist, already exists. "
                          + " is there another bookie process running?", fn);
                throw new IOException("File " + fn
                        + " suddenly appeared, is another bookie process running?");
            }
            fc = new RandomAccessFile(fn, "rw").getChannel();
            formatVersion = CURRENT_JOURNAL_FORMAT_VERSION;

            ByteBuffer bb = ByteBuffer.allocate(HEADER_SIZE);
            bb.put(MAGIC_WORD);
            bb.putInt(formatVersion);
            bb.flip();
            fc.write(bb);
            fc.force(true);

            bc = new BufferedChannel(fc, 65536);

            nextPrealloc = preAllocSize;
            fc.write(zeros, nextPrealloc);
        } else {  // open an existing file
            fc = new RandomAccessFile(fn, "r").getChannel();
            bc = null; // readonly

            ByteBuffer bb = ByteBuffer.allocate(HEADER_SIZE);
            int c = fc.read(bb);
            bb.flip();

            if (c == HEADER_SIZE) {
                byte[] first4 = new byte[4];
                bb.get(first4);

                if (Arrays.equals(first4, MAGIC_WORD)) {
                    formatVersion = bb.getInt();
                } else {
                    // pre magic word journal, reset to 0;
                    formatVersion = 1;
                }
            } else {
                // no header, must be old version
                formatVersion = 1;
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
                    if (formatVersion >= 2) {
                        fc.position(HEADER_SIZE);
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

    void preAllocIfNeeded() throws IOException {
        if (bc.position() > nextPrealloc) {
            nextPrealloc = ((fc.size() + HEADER_SIZE) / preAllocSize + 1) * preAllocSize;
            zeros.clear();
            fc.write(zeros, nextPrealloc);
        }
    }

    int read(ByteBuffer dst)
            throws IOException {
        return fc.read(dst);
    }

    public void close() throws IOException {
        fc.close();
    }
}
