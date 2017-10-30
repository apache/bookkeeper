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

/**
 * Marks Handles which support the 'sync' primitive
 *
 * @see WriteHandle
 * @see WriteAdvHandle
 *
 * @since 4.6
 */
public interface ForceSupported {

    /**
     * Forces the bookie to persist durably data sent from this client to the ledger and advances
     * the LastAddConfirmed client side pointer.
     *
     * In case of volatile durability ledgers, for instance {@link LedgerType#FORCE_DEFERRED_ON_JOURNAL}, this operation is
     * required in order to let the LastAddConfirmed pointer to advance.
     * <p>
     * <b>Beware that closing a volatile durability ledger does not imply a sync operation</b>
     * <p>
     * Even without calling this primitive entry could be readable using {@link ReadHandle#readUnconfirmed(long, long) }
     * function
     *     
     * @return an handle to the result, in case of success it will return the id of last persisted entry id
     */
    CompletableFuture<Long> force();

}
