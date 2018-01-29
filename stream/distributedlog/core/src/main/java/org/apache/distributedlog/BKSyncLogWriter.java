/**
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

import java.io.IOException;
import java.util.List;
import org.apache.distributedlog.api.LogWriter;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.util.Utils;

class BKSyncLogWriter extends BKAbstractLogWriter implements LogWriter {

    public BKSyncLogWriter(DistributedLogConfiguration conf,
                           DynamicDistributedLogConfiguration dynConf,
                           BKDistributedLogManager bkdlm) {
        super(conf, dynConf, bkdlm);
    }
    /**
     * Write log records to the stream.
     *
     * @param record operation
     */
    @Override
    public void write(LogRecord record) throws IOException {
        getLedgerWriter(record.getTransactionId(), false).write(record);
    }

    /**
     * Write edits logs operation to the stream.
     *
     * @param records list of records
     */
    @Override
    @Deprecated
    public int writeBulk(List<LogRecord> records) throws IOException {
        return getLedgerWriter(records.get(0).getTransactionId(), false).writeBulk(records);
    }

    /**
     * Flushes all the data up to this point,
     * adds the end of stream marker and marks the stream
     * as read-only in the metadata. No appends to the
     * stream will be allowed after this point
     */
    @Override
    public void markEndOfStream() throws IOException {
        Utils.ioResult(getLedgerWriter(DistributedLogConstants.MAX_TXID, true).markEndOfStream());
        closeAndComplete();
    }

    /**
     * All data that has been written to the stream so far will be flushed.
     * New data can be still written to the stream while flush is ongoing.
     */
    @Override
    public long flush() throws IOException {
        checkClosedOrInError("flush");
        long highestTransactionId = 0;
        BKLogSegmentWriter writer = getCachedLogWriter();
        if (null != writer) {
            highestTransactionId = Math.max(highestTransactionId, Utils.ioResult(writer.flush()));
        }
        return highestTransactionId;
    }

    /**
     * Commit data that is already flushed.
     * <p/>
     * This API is optional as the writer implements a policy for automatically syncing
     * the log records in the buffer. The buffered edits can be flushed when the buffer
     * becomes full or a certain period of time is elapsed.
     */
    @Override
    public long commit() throws IOException {
        checkClosedOrInError("commit");

        LOG.debug("FlushAndSync Started");
        long highestTransactionId = 0;
        BKLogSegmentWriter writer = getCachedLogWriter();
        if (null != writer) {
            highestTransactionId = Math.max(highestTransactionId, Utils.ioResult(writer.commit()));
            LOG.debug("FlushAndSync Completed");
        } else {
            LOG.debug("FlushAndSync Completed - Nothing to Flush");
        }
        return highestTransactionId;
    }

    /**
     * Close the stream without necessarily flushing immediately.
     * This may be called if the stream is in error such as after a
     * previous write or close threw an exception.
     */
    @Override
    public void abort() throws IOException {
        super.abort();
    }
}
