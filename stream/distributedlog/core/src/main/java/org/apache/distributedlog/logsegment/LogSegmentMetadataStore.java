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
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.callback.LogSegmentNamesListener;
import org.apache.distributedlog.metadata.LogMetadata;
import org.apache.distributedlog.metadata.LogMetadataForWriter;
import org.apache.distributedlog.util.Transaction;
import org.apache.distributedlog.util.Transaction.OpListener;


/**
 * Interface for log segment metadata store. All operations that modify log segments should
 * be executed under a {@link Transaction}.
 */
@Beta
public interface LogSegmentMetadataStore extends Closeable {

    /**
     * Start the transaction on changing log segment metadata store.
     *
     * @return transaction of the log segment metadata store.
     */
    Transaction<Object> transaction();

    // The reason to keep storing log segment sequence number & log record transaction id
    // in this log segment metadata store interface is to share the transaction that used
    // to start/complete log segment. It is a bit hard to separate them out right now.

    /**
     * Store the maximum log segment sequence number on <code>path</code>.
     *
     * @param txn
     *          transaction to execute for storing log segment sequence number.
     * @param logMetadata
     *          metadata of the log stream
     * @param sequenceNumber
     *          log segment sequence number to store
     * @param listener
     *          listener on the result to this operation
     */
    void storeMaxLogSegmentSequenceNumber(Transaction<Object> txn,
                                          LogMetadata logMetadata,
                                          Versioned<Long> sequenceNumber,
                                          OpListener<Version> listener);

    /**
     * Store the maximum transaction id for <code>path</code>.
     *
     * @param txn
     *          transaction to execute for storing transaction id
     * @param logMetadata
     *          metadata of the log stream
     * @param transactionId
     *          transaction id to store
     * @param listener
     *          listener on the result to this operation
     */
    void storeMaxTxnId(Transaction<Object> txn,
                       LogMetadataForWriter logMetadata,
                       Versioned<Long> transactionId,
                       OpListener<Version> listener);

    /**
     * Create a log segment <code>segment</code> under transaction <code>txn</code>.
     *
     * <p>NOTE: this operation shouldn't be a blocking call. and it shouldn't execute the operation
     *       immediately. the operation should be executed via {@link Transaction#execute()}</p>
     *
     * @param txn
     *          transaction to execute for this operation
     * @param segment
     *          segment to create
     * @param opListener
     *          the listener on the operation result
     */
    void createLogSegment(Transaction<Object> txn,
                          LogSegmentMetadata segment,
                          OpListener<Void> opListener);

    /**
     * Delete a log segment <code>segment</code> under transaction <code>txn</code>.
     *
     * <p>NOTE: this operation shouldn't be a blocking call. and it shouldn't execute the operation
     *       immediately. the operation should be executed via {@link Transaction#execute()}</p>
     *
     * @param txn
     *          transaction to execute for this operation
     * @param segment
     *          segment to delete
     */
    void deleteLogSegment(Transaction<Object> txn,
                          LogSegmentMetadata segment,
                          OpListener<Void> opListener);

    /**
     * Update a log segment <code>segment</code> under transaction <code>txn</code>.
     *
     * <p>NOTE: this operation shouldn't be a blocking call. and it shouldn't execute the operation
     *       immediately. the operation should be executed via {@link Transaction#execute()}</p>
     *
     * @param txn
     *          transaction to execute for this operation
     * @param segment
     *          segment to update
     */
    void updateLogSegment(Transaction<Object> txn, LogSegmentMetadata segment);

    /**
     * Retrieve the log segment associated <code>path</code>.
     *
     * @param logSegmentPath
     *          path to store log segment metadata
     * @return future of the retrieved log segment metadata
     */
    CompletableFuture<LogSegmentMetadata> getLogSegment(String logSegmentPath);

    /**
     * Retrieve the list of log segments under <code>logSegmentsPath</code> and register a <i>listener</i>
     * for subsequent changes for the list of log segments.
     *
     * @param logSegmentsPath
     *          path to store list of log segments
     * @param listener
     *          log segment listener on log segment changes
     * @return future of the retrieved list of log segment names
     */
    CompletableFuture<Versioned<List<String>>> getLogSegmentNames(String logSegmentsPath,
                                                                  LogSegmentNamesListener listener);

    /**
     * Unregister a log segment <code>listener</code> on log segment changes under <code>logSegmentsPath</code>.
     *
     * @param logSegmentsPath
     *          log segments path
     * @param listener
     *          log segment listener on log segment changes
     */
    void unregisterLogSegmentListener(String logSegmentsPath,
                                      LogSegmentNamesListener listener);
}
