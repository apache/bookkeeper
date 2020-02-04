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
package org.apache.distributedlog.api;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.AppendOnlyStreamReader;
import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.api.subscription.SubscriptionsStore;
import org.apache.distributedlog.bk.LedgerMetadata;
import org.apache.distributedlog.callback.LogSegmentListener;
import org.apache.distributedlog.io.AsyncCloseable;
import org.apache.distributedlog.namespace.NamespaceDriver;

/**
 * A DistributedLogManager is responsible for managing a single place of storing
 * edit logs. It may correspond to multiple files, a backup node, etc.
 * Even when the actual underlying storage is rolled, or failed and restored,
 * each conceptual place of storage corresponds to exactly one instance of
 * this class, which is created when the EditLog is first opened.
 */
@Public
@Evolving
public interface DistributedLogManager extends AsyncCloseable, Closeable {

    /**
     * Get the name of the stream managed by this log manager.
     * @return streamName
     */
    String getStreamName();

    /**
     * Get the namespace driver used by this manager.
     *
     * @return the namespace driver
     */
    NamespaceDriver getNamespaceDriver();

    //
    // Log Segment Related Operations
    //

    /**
     * Get log segments.
     *
     * @return log segments
     * @throws IOException
     */
    List<LogSegmentMetadata> getLogSegments() throws IOException;


    /**
     * Get the log segments asynchronously.
     *
     * @return the log segments
     */
    CompletableFuture<List<LogSegmentMetadata>> getLogSegmentsAsync();

    /**
     * Register <i>listener</i> on log segment updates of this stream.
     *
     * @param listener
     *          listener to receive update log segment list.
     */
    void registerListener(LogSegmentListener listener) throws IOException;

    /**
     * Unregister <i>listener</i> on log segment updates from this stream.
     *
     * @param listener
     *          listener to receive update log segment list.
     */
    void unregisterListener(LogSegmentListener listener);

    //
    // Writer & Reader Operations
    //

    /**
     * Open async log writer to write records to the log stream.
     *
     * @return result represents the open result
     */
    CompletableFuture<AsyncLogWriter> openAsyncLogWriter();

    /**
     * Open async log writer to write records to the log stream.
     * Provided metadata will be attached to the underlying BookKeeper ledgers.
     *
     * @param ledgerMetadata
     * @return result represents the open result
     */
    default CompletableFuture<AsyncLogWriter> openAsyncLogWriter(LedgerMetadata ledgerMetadata) {
        return FutureUtils.exception(new UnsupportedOperationException());
    }

    /**
     * Open sync log writer to write records to the log stream.
     *
     * @return sync log writer
     * @throws IOException when fails to open a sync log writer.
     */
    LogWriter openLogWriter() throws IOException;

    /**
     * Open sync log writer to write records to the log stream.
     * Provided metadata will be attached to the underlying BookKeeper ledgers.
     *
     * @param ledgerMetadata
     * @return sync log writer
     * @throws IOException when fails to open a sync log writer.
     */
    default LogWriter openLogWriter(LedgerMetadata ledgerMetadata) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Begin writing to the log stream identified by the name.
     *
     * @return the writer interface to generate log records
     * @Deprecated since 0.5.0, in favor of using {@link #openLogWriter()}
     */
    LogWriter startLogSegmentNonPartitioned() throws IOException;

    /**
     * Begin writing to the log stream identified by the name.
     *
     * @return the writer interface to generate log records
     * @Deprecated since 0.5.0, in favor of using {@link #openAsyncLogWriter()}
     */
    AsyncLogWriter startAsyncLogSegmentNonPartitioned() throws IOException;

    /**
     * Open an sync log reader to read records from a log starting from <code>fromTxnId</code>.
     *
     * @param fromTxnId
     *          transaction id to start reading from
     * @return sync log reader
     */
    LogReader openLogReader(long fromTxnId) throws IOException;

    /**
     * Open an async log reader to read records from a log starting from <code>fromDLSN</code>.
     *
     * @param fromDLSN
     *          dlsn to start reading from
     * @return async log reader
     */
    LogReader openLogReader(DLSN fromDLSN) throws IOException;

    /**
     * Open an async log reader to read records from a log starting from <code>fromTxnId</code>.
     *
     * @param fromTxnId
     *          transaction id to start reading from
     * @return async log reader
     */
    CompletableFuture<AsyncLogReader> openAsyncLogReader(long fromTxnId);

    /**
     * Open an async log reader to read records from a log starting from <code>fromDLSN</code>.
     *
     * @param fromDLSN
     *          dlsn to start reading from
     * @return async log reader
     */
    CompletableFuture<AsyncLogReader> openAsyncLogReader(DLSN fromDLSN);

    /**
     * Get the input stream starting with fromTxnId for the specified log.
     *
     * @param fromTxnId - the first transaction id we want to read
     * @return the stream starting with transaction fromTxnId
     * @throws IOException if a stream cannot be found.
     * @Deprecated since 0.5.0, in favor of using {@link #openLogReader(long)}
     */
    LogReader getInputStream(long fromTxnId)
        throws IOException;

    /**
     * Get the input stream starting with fromTxnId for the specified log.
     *
     * @param fromDLSN - the first DLSN we want to read
     * @return the stream starting with DLSN
     * @throws IOException if a stream cannot be found.
     * @Deprecated since 0.5.0, in favor of using {@link #openLogReader(DLSN)}
     */
    LogReader getInputStream(DLSN fromDLSN) throws IOException;

    /**
     * Get an async log reader to read records from a log starting from <code>fromTxnId</code>.
     *
     * @param fromTxnId
     *          transaction id to start reading from
     * @return async log reader
     * @throws IOException when fails to open an async log reader.
     * @see #openAsyncLogReader(long)
     * @Deprecated it is deprecated since 0.5.0, in favor of using {@link #openAsyncLogReader(long)}
     */
    AsyncLogReader getAsyncLogReader(long fromTxnId) throws IOException;

    /**
     * Get an async log reader to read records from a log starting from <code>fromDLSN</code>.
     *
     * @param fromDLSN
     *          dlsn to start reading from
     * @return async log reader
     * @throws IOException when fails to open an async log reader.
     * @see #openAsyncLogReader(DLSN)
     * @Deprecated it is deprecated since 0.5.0, in favor of using {@link #openAsyncLogReader(DLSN)}
     */
    AsyncLogReader getAsyncLogReader(DLSN fromDLSN) throws IOException;

    /**
     * Get a log reader with lock starting from <i>fromDLSN</i>.
     *
     * <p>If two readers tried to open using same subscriberId, one would succeed, while the other
     * will be blocked until it gets the lock.
     *
     * @param fromDLSN
     *          start dlsn
     * @return async log reader
     */
    CompletableFuture<AsyncLogReader> getAsyncLogReaderWithLock(DLSN fromDLSN);

    /**
     * Get a log reader with lock starting from <i>fromDLSN</i> and using <i>subscriberId</i>.
     *
     * <p>If two readers tried to open using same subscriberId, one would succeed, while the other
     * will be blocked until it gets the lock.
     *
     * @param fromDLSN
     *          start dlsn
     * @param subscriberId
     *          subscriber id
     * @return async log reader
     */
    CompletableFuture<AsyncLogReader> getAsyncLogReaderWithLock(DLSN fromDLSN, String subscriberId);

    /**
     * Get a log reader using <i>subscriberId</i> with lock. The reader will start reading from
     * its last commit position recorded in subscription store. If no last commit position found
     * in subscription store, it would start reading from head of the stream.
     *
     * <p>If the two readers tried to open using same subscriberId, one would succeed, while the other
     * will be blocked until it gets the lock.
     *
     * @param subscriberId
     *          subscriber id
     * @return async log reader
     */
    CompletableFuture<AsyncLogReader> getAsyncLogReaderWithLock(String subscriberId);

    //
    // Stream writer and reader
    //

    /**
     * Begin appending to the end of the log stream which is being treated as a sequence of bytes.
     *
     * @return the writer interface to generate log records
     */
    AppendOnlyStreamWriter getAppendOnlyStreamWriter() throws IOException;

    /**
     * Get a reader to read a log stream as a sequence of bytes.
     *
     * @return the writer interface to generate log records
     */
    AppendOnlyStreamReader getAppendOnlyStreamReader() throws IOException;

    //
    // Metadata Operations:
    //
    // - retrieve head or tail records
    // - get log record count
    // - delete logs
    // - recover logs
    //

    /**
     * Get the {@link DLSN} of first log record whose transaction id is not less than <code>transactionId</code>.
     *
     * @param transactionId
     *          transaction id
     * @return dlsn of first log record whose transaction id is not less than transactionId.
     */
    CompletableFuture<DLSN> getDLSNNotLessThanTxId(long transactionId);

    /**
     * Get the last log record in the stream.
     *
     * @return the last log record in the stream
     * @throws IOException if a stream cannot be found.
     */
    LogRecordWithDLSN getLastLogRecord()
        throws IOException;

    /**
     * Get Latest log record with DLSN in the log - async.
     *
     * @return latest log record with DLSN
     */
    CompletableFuture<LogRecordWithDLSN> getLastLogRecordAsync();

    /**
     * Get the first log record in the stream.
     *
     * @return the first log record in the stream
     * @throws IOException if a stream cannot be found.
     */
    LogRecordWithDLSN getFirstLogRecord()
        throws IOException;

    /**
     * Get first log record with DLSN in the log - async.
     *
     * @return latest log record with DLSN
     */
    CompletableFuture<LogRecordWithDLSN> getFirstLogRecordAsync();

    /**
     * Get the earliest Transaction Id available in the log.
     *
     * @return earliest transaction id
     * @throws IOException
     */
    long getFirstTxId() throws IOException;

    /**
     * Get Latest Transaction Id in the log.
     *
     * @return latest transaction id
     * @throws IOException
     */
    long getLastTxId() throws IOException;

    /**
     * Get Latest Transaction Id in the log - async.
     *
     * @return latest transaction id
     */
    CompletableFuture<Long> getLastTxIdAsync();

    /**
     * Get first DLSN in the log.
     *
     * @return first dlsn in the stream
     */
    CompletableFuture<DLSN> getFirstDLSNAsync();

    /**
     * Get Latest DLSN in the log.
     *
     * @return last dlsn
     * @throws IOException
     */
    DLSN getLastDLSN() throws IOException;

    /**
     * Get Latest DLSN in the log - async.
     *
     * @return latest transaction id
     */
    CompletableFuture<DLSN> getLastDLSNAsync();

    /**
     * Get the number of log records in the active portion of the log.
     * Any log segments that have already been truncated will not be included
     *
     * @return number of log records
     * @throws IOException
     */
    long getLogRecordCount() throws IOException;

    /**
     * Get the number of log records in the active portion of the log - async.
     * Any log segments that have already been truncated will not be included
     *
     * @return future number of log records
     * @throws IOException
     */
    CompletableFuture<Long> getLogRecordCountAsync(DLSN beginDLSN);

    /**
     * Run recovery on the log.
     *
     * @throws IOException
     */
    void recover() throws IOException;

    /**
     * Check if an end of stream marker was added to the stream.
     * A stream with an end of stream marker cannot be appended to
     *
     * @return true if the marker was added to the stream, false otherwise
     * @throws IOException
     */
    boolean isEndOfStreamMarked() throws IOException;

    /**
     * Delete the log.
     *
     * @throws IOException if the deletion fails
     * @Deprecated since 0.5.0, in favor of using
     *             {@link org.apache.distributedlog.api.namespace.Namespace#deleteLog(String)}
     */
    void delete() throws IOException;

    /**
     * The DistributedLogManager may archive/purge any logs for transactionId
     * less than or equal to minImageTxId.
     * This is to be used only when the client explicitly manages deletion. If
     * the cleanup policy is based on sliding time window, then this method need
     * not be called.
     *
     * @param minTxIdToKeep the earliest txid that must be retained
     * @throws IOException if purging fails
     */
    void purgeLogsOlderThan(long minTxIdToKeep) throws IOException;

    /**
     * Get the subscriptions store provided by the distributedlog manager.
     *
     * @return subscriptions store manages subscriptions for current stream.
     */
    SubscriptionsStore getSubscriptionsStore();

}
