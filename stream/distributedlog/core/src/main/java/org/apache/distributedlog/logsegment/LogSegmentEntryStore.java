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
package org.apache.distributedlog.logsegment;

import com.google.common.annotations.Beta;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.bk.LedgerMetadata;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.metadata.LogMetadataForWriter;
import org.apache.distributedlog.util.Allocator;


/**
 * Log Segment Store to read log segments.
 */
@Beta
public interface LogSegmentEntryStore {

    /**
     * Delete the actual log segment from the entry store.
     *
     * @param segment log segment metadata
     * @return future represent the delete result
     */
    CompletableFuture<LogSegmentMetadata> deleteLogSegment(LogSegmentMetadata segment);

    /**
     * Create a new log segment allocator for allocating log segment entry writers.
     *
     * @param metadata the metadata for the log stream
     * @return future represent the log segment allocator
     */
    Allocator<LogSegmentEntryWriter, Object> newLogSegmentAllocator(
            LogMetadataForWriter metadata,
            DynamicDistributedLogConfiguration dynConf) throws IOException;

    /**
     * Create a new log segment allocator for allocating log segment entry writers.
     *
     * @param metadata the metadata for the log stream
     * @param ledgerMetadata metadata to be attached to underlying ledgers
     * @return future represent the log segment allocator
     */
    default Allocator<LogSegmentEntryWriter, Object> newLogSegmentAllocator(
            LogMetadataForWriter metadata,
            DynamicDistributedLogConfiguration dynConf,
            LedgerMetadata ledgerMetadata) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Open the reader for reading data to the log <i>segment</i>.
     *
     * @param segment the log <i>segment</i> to read data from
     * @param startEntryId the start entry id
     * @return future represent the opened reader
     */
    CompletableFuture<LogSegmentEntryReader> openReader(LogSegmentMetadata segment,
                                             long startEntryId);

    /**
     * Open the reader for reading entries from a random access log <i>segment</i>.
     *
     * @param segment the log <i>segment</i> to read entries from
     * @param fence the flag to fence log segment
     * @return future represent the opened random access reader
     */
    CompletableFuture<LogSegmentRandomAccessEntryReader> openRandomAccessReader(LogSegmentMetadata segment,
                                                                     boolean fence);
}
