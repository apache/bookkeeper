/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.dlog;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;

/**
 * The input stream for a distributedlog stream.
 */
@Slf4j
class DLInputStream extends InputStream {

    private static final long REOPEN_READER_SKIP_BYTES = 4 * 1024 * 1024; // 4MB

    private static class RecordStream {

        private final InputStream payloadStream;
        private final LogRecordWithDLSN record;

        RecordStream(LogRecordWithDLSN record) {
            checkNotNull(record);

            this.record = record;
            this.payloadStream = record.getPayLoadInputStream();
        }

    }

    private static RecordStream nextRecordStream(LogReader reader) throws IOException {
        LogRecordWithDLSN record = reader.readNext(false);
        if (null != record) {
            return new RecordStream(record);
        }
        return null;
    }

    private final DistributedLogManager dlm;
    private LogReader reader;
    private long pos;
    private long lastPos;
    private RecordStream currentRecord = null;

    DLInputStream(DistributedLogManager dlm,
                  LogReader reader,
                  long startPos)
            throws IOException {
        this.dlm = dlm;
        this.reader = reader;
        this.pos = startPos;
        this.lastPos = readEndPos();
        seek(startPos);
    }

    @Override
    public void close() throws IOException {
        reader.close();
        dlm.close();
    }

    private long readEndPos() throws IOException {
        return dlm.getLastTxId();
    }

    //
    // FSInputStream
    //

    public void seek(long pos) throws IOException {
        if (this.pos == pos) {
            return;
        }

        if (this.pos > pos || (pos - this.pos) >= REOPEN_READER_SKIP_BYTES) {
            // close the previous reader
            this.reader.close();
            this.reader = dlm.openLogReader(pos);
            this.currentRecord = null;
        }

        skipTo(pos);
    }

    private boolean skipTo(final long position) throws IOException {
        while (true) {
            if (null == currentRecord) {
                currentRecord = nextRecordStream(reader);
            }

            if (null == currentRecord) { // the stream is empty now
                return false;
            }

            long endPos = currentRecord.record.getTransactionId();
            if (endPos < position) {
                currentRecord = nextRecordStream(reader);
                this.pos = endPos;
                continue;
            } else if (endPos == position){
                // find the record, but we defer read next record when actual read happens
                this.pos = position;
                this.currentRecord = null;
                return true;
            } else {
                this.currentRecord.payloadStream.skip(
                    this.currentRecord.payloadStream.available() - (endPos - position));
                this.pos = position;
                return true;
            }
        }
    }

    //
    // Input Stream
    //

    @Override
    public int read(byte[] b, final int off, final int len) throws IOException {
        int remaining = len;
        int numBytesRead = 0;
        while (remaining > 0) {
            if (null == currentRecord) {
                currentRecord = nextRecordStream(reader);
            }

            if (null == currentRecord) {
                if (numBytesRead == 0) {
                    return -1;
                }
                break;
            }

            int bytesLeft = currentRecord.payloadStream.available();
            if (bytesLeft <= 0) {
                currentRecord.payloadStream.close();
                currentRecord = null;
                continue;
            }

            int numBytesToRead = Math.min(bytesLeft, remaining);
            int numBytes = currentRecord.payloadStream.read(b, off + numBytesRead, numBytesToRead);
            if (numBytes < 0) {
                continue;
            }
            numBytesRead += numBytes;
            remaining -= numBytes;
        }
        return numBytesRead;
    }

    @Override
    public long skip(final long n) throws IOException {
        if (n <= 0L) {
            return 0L;
        }

        long remaining = n;
        while (true) {
            if (null == currentRecord) {
                currentRecord = nextRecordStream(reader);
            }

            if (null == currentRecord) { // end of stream
                return n - remaining;
            }

            int bytesLeft = currentRecord.payloadStream.available();
            long endPos = currentRecord.record.getTransactionId();
            if (remaining > bytesLeft) {
                // skip the whole record
                remaining -= bytesLeft;
                this.pos = endPos;
                this.currentRecord = nextRecordStream(reader);
                continue;
            } else if (remaining == bytesLeft) {
                this.pos = endPos;
                this.currentRecord = null;
                return n;
            } else {
                currentRecord.payloadStream.skip(remaining);
                this.pos = endPos - currentRecord.payloadStream.available();
                return n;
            }
        }
    }

    @Override
    public int available() throws IOException {
        if (lastPos - pos == 0L) {
            lastPos = readEndPos();
        }
        return (int) (lastPos - pos);
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public int read() throws IOException {
        byte[] data = new byte[1];
        int numBytes = read(data);
        if (numBytes <= 0) {
            return -1;
        }
        return data[0];
    }

}
