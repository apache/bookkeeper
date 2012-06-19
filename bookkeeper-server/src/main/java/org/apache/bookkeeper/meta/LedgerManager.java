package org.apache.bookkeeper.meta;

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

import java.io.Closeable;

import org.apache.zookeeper.AsyncCallback;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;

/**
 * LedgerManager takes responsibility of ledger management in client side.
 *
 * <ul>
 * <li>How to store ledger meta (e.g. in ZooKeeper or other key/value store)
 * </ul>
 */
public interface LedgerManager extends Closeable {

    /**
     * Create a new ledger with provided metadata
     *
     * @param metadata
     *        Metadata provided when creating a new ledger
     * @param cb
     *        Callback when creating a new ledger.
     */
    public abstract void createLedger(LedgerMetadata metadata, GenericCallback<Long> cb);

    /**
     * Delete a specified ledger by ledgerId.
     *
     * @param ledgerId
     *          Ledger Id
     * @param cb
     *          Callback when deleted ledger.
     */
    public abstract void deleteLedger(long ledgerId, GenericCallback<Void> cb);

    /**
     * Read ledger metadata of a specified ledger.
     *
     * @param ledgerId
     *          Ledger Id
     * @param readCb
     *          Callback when read ledger metadata.
     */
    public abstract void readLedgerMetadata(long ledgerId, GenericCallback<LedgerMetadata> readCb);

    /**
     * Write ledger metadata.
     *
     * @param ledgerId
     *          Ledger Id
     * @param metadata
     *          Ledger Metadata to write
     * @param cb
     *          Callback when finished writing ledger metadata.
     */
    public abstract void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<Void> cb);

    /**
     * Loop to process all ledgers.
     * <p>
     * <ul>
     * After all ledgers were processed, finalCb will be triggerred:
     * <li> if all ledgers are processed done with OK, success rc will be passed to finalCb.
     * <li> if some ledgers are prcoessed failed, failure rc will be passed to finalCb.
     * </ul>
     * </p>
     *
     * @param processor
     *          Ledger Processor to process a specific ledger
     * @param finalCb
     *          Callback triggered after all ledgers are processed
     * @param context
     *          Context of final callback
     * @param successRc
     *          Success RC code passed to finalCb when callback
     * @param failureRc
     *          Failure RC code passed to finalCb when exceptions occured.
     */
    public void asyncProcessLedgers(Processor<Long> processor, AsyncCallback.VoidCallback finalCb,
                                    Object context, int successRc, int failureRc);
}
