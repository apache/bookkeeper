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
package org.apache.distributedlog.metadata;

import com.google.common.annotations.Beta;
import java.io.Closeable;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.common.util.PermitManager;
import org.apache.distributedlog.lock.DistributedLock;
import org.apache.distributedlog.logsegment.LogSegmentMetadataStore;
import org.apache.distributedlog.util.Transaction;



/**
 * The interface to manage the log stream metadata. The implementation is responsible
 * for creating the metadata layout.
 */
@Beta
public interface LogStreamMetadataStore extends Closeable {

    /**
     * Create a transaction for the metadata operations happening in the metadata store.
     *
     * @return transaction for the metadata operations
     */
    Transaction<Object> newTransaction();

    /**
     * Ensure the existence of a log stream.
     *
     * @param uri the location of the log stream
     * @param logName the name of the log stream
     * @return future represents the existence of a log stream.
     *         {@link org.apache.distributedlog.exceptions.LogNotFoundException} is thrown if the log doesn't exist
     */
    CompletableFuture<Void> logExists(URI uri, String logName);

    /**
     * Create the read lock for the log stream.
     *
     * @param metadata the metadata for a log stream
     * @param readerId the reader id used for lock
     * @return the read lock
     */
    CompletableFuture<DistributedLock> createReadLock(LogMetadataForReader metadata,
                                           Optional<String> readerId);

    /**
     * Create the write lock for the log stream.
     *
     * @param metadata the metadata for a log stream
     * @return the write lock
     */
    DistributedLock createWriteLock(LogMetadataForWriter metadata);

    /**
     * Create the metadata of a log.
     *
     * @param uri the location to store the metadata of the log
     * @param streamName the name of the log stream
     * @param ownAllocator whether to use its own allocator or external allocator
     * @param createIfNotExists flag to create the stream if it doesn't exist
     * @return the metadata of the log
     */
    CompletableFuture<LogMetadataForWriter> getLog(URI uri,
                                        String streamName,
                                        boolean ownAllocator,
                                        boolean createIfNotExists);

    /**
     * Delete the metadata of a log.
     *
     * @param uri the location to store the metadata of the log
     * @param streamName the name of the log stream
     * @return future represents the result of the deletion.
     */
    CompletableFuture<Void> deleteLog(URI uri, String streamName);

    /**
     * Rename the log from <i>oldStreamName</i> to <i>newStreamName</i>.
     *
     * @param uri the location to store the metadata of the log
     * @param oldStreamName the old name of the log stream
     * @param newStreamName the new name of the log stream
     * @return future represents the result of the rename operation.
     */
    CompletableFuture<Void> renameLog(URI uri,
                                      String oldStreamName,
                                      String newStreamName);

    /**
     * Get the log segment metadata store.
     *
     * @return the log segment metadata store.
     */
    LogSegmentMetadataStore getLogSegmentMetadataStore();

    /**
     * Get the permit manager for this metadata store. It can be used for limiting the concurrent
     * metadata operations. The implementation can disable handing over the permits when the metadata
     * store is unavailable (for example zookeeper session expired).
     *
     * @return the permit manager
     */
    PermitManager getPermitManager();



}
