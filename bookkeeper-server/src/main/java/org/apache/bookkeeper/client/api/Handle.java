/**
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
package org.apache.bookkeeper.client.api;

import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * Handle to manage an open ledger.
 *
 * @since 4.6
 */
@Public
@Unstable
public interface Handle extends AutoCloseable {

    /**
     * Get the id of the current ledger.
     *
     * @return the id of the ledger
     */
    long getId();

    /**
     * Close this ledger synchronously.
     *
     * @throws org.apache.bookkeeper.client.api.BKException
     * @throws java.lang.InterruptedException
     * @see #asyncClose
     */
    @Override
    @SneakyThrows(Exception.class)
    default void close() throws BKException, InterruptedException {
        FutureUtils.result(asyncClose());
    }

    /**
     * Returns the metadata of this ledger.
     *
     * <p>This call only retrieves the metadata cached locally. If there is any metadata updated, the read
     * handle will receive the metadata updates and update the metadata locally. The metadata notification
     * can be deplayed, so it is possible you can receive a stale copy of ledger metadata from this call.
     *
     * @return the metadata of this ledger.
     */
    LedgerMetadata getLedgerMetadata();

    /**
     * Asynchronous close, any adds in flight will return errors.
     *
     * <p>Closing a ledger will ensure that all clients agree on what the last
     * entry of the ledger is. This ensures that, once the ledger has been closed,
     * all reads from the ledger will return the same set of entries.
     *
     * @return an handle to access the result of the operation
     *
     * @see FutureUtils#result(java.util.concurrent.CompletableFuture) to have a simple method to access the result
     */
    CompletableFuture<Void> asyncClose();

}
