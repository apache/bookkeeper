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
package org.apache.distributedlog;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Record reader to read records from an enveloped entry buffer.
 */
@NotThreadSafe
class EnvelopedEntryReader implements Entry.Reader, RecordStream {

    private final long logSegmentSeqNo;
    private final long entryId;
    private final ByteBuf src;
    private final LogRecord.Reader reader;

    // slot id
    private long slotId = 0;
    // error lag
    private IOException lastException = null;
    private boolean isExhausted = false;

    EnvelopedEntryReader(long logSegmentSeqNo,
                         long entryId,
                         long startSequenceId,
                         ByteBuf in,
                         boolean envelopedEntry,
                         boolean deserializeRecordSet,
                         StatsLogger statsLogger)
            throws IOException {
        this.logSegmentSeqNo = logSegmentSeqNo;
        this.entryId = entryId;
        if (envelopedEntry) {
            this.src = EnvelopedEntry.fromEnvelopedBuf(in, statsLogger);
        } else {
            this.src = in;
        }
        this.reader = new LogRecord.Reader(
                this,
                src,
                startSequenceId,
                deserializeRecordSet);
    }

    @VisibleForTesting
    boolean isExhausted() {
        return isExhausted;
    }

    @VisibleForTesting
    ByteBuf getSrcBuf() {
        return src;
    }

    private void checkLastException() throws IOException {
        if (null != lastException) {
            throw lastException;
        }
    }

    private void releaseBuffer() {
        isExhausted = true;
        ReferenceCountUtil.release(this.src);
    }

    @Override
    public long getLSSN() {
        return logSegmentSeqNo;
    }

    @Override
    public long getEntryId() {
        return entryId;
    }

    @Override
    public LogRecordWithDLSN nextRecord() throws IOException {
        checkLastException();

        if (isExhausted) {
            return null;
        }

        LogRecordWithDLSN record;
        try {
            record = reader.readOp();
        } catch (IOException ioe) {
            lastException = ioe;
            releaseBuffer();
            throw ioe;
        }
        if (null == record) {
            releaseBuffer();
        }
        return record;
    }

    public void release() {
        if (isExhausted) {
            return;
        }
        releaseBuffer();
    }

    @Override
    public boolean skipTo(long txId) throws IOException {
        checkLastException();

        return reader.skipTo(txId, true);
    }

    @Override
    public boolean skipTo(DLSN dlsn) throws IOException {
        checkLastException();

        return reader.skipTo(dlsn);
    }

    //
    // Record Stream
    //

    @Override
    public void advance(int numRecords) {
        slotId += numRecords;
    }

    @Override
    public DLSN getCurrentPosition() {
        return new DLSN(logSegmentSeqNo, entryId, slotId);
    }

    @Override
    public String getName() {
        return "EnvelopedReader";
    }

}
