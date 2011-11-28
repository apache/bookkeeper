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

import java.io.IOException;

import org.apache.zookeeper.AsyncCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;

/**
 * LedgerManager takes responsibility of ledger management
 *
 * <ul>
 * <li>How to store ledger meta (e.g. in ZooKeeper or other key/value store)
 * <li>How to manager active ledgers (so know how to do garbage collection)
 * <li>How to garbage collect inactive/deleted ledgers
 * </ul>
 */
public interface LedgerManager {

    /**
     * Get the path that is used to store ledger metadata
     *
     * @param ledgerId
     *          Ledger ID
     * @return ledger node path
     */
    public String getLedgerPath(long ledgerId);

    /**
     * Get ledger id from its ledger path
     *
     * @param ledgerPath
     *          Ledger path to store metadata
     * @return ledger id
     * @throws IOException when the ledger path is invalid
     */
    public long getLedgerId(String ledgerPath) throws IOException;

    /**
     * Create a new zk ledger path.
     *
     * @param cb
     *        Callback when getting new zk ledger path to create.
     */
    public abstract void newLedgerPath(GenericCallback<String> cb);

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

    /**
     * Add active ledger
     *
     * @param ledgerId
     *          Ledger ID
     * @param active
     *          Status of ledger
     */
    public void addActiveLedger(long ledgerId, boolean active);

    /**
     * Remove active ledger
     *
     * @param ledgerId
     *          Ledger ID
     */
    public void removeActiveLedger(long ledgerId);

    /**
     * Is Ledger ledgerId in active ledgers set
     *
     * @param ledgerId
     *          Ledger ID
     * @return true if the ledger is in active ledgers set, otherwise return false
     */
    public boolean containsActiveLedger(long ledgerId);

    /**
     * Garbage Collector which handles ledger deletion in server side
     */
    public static interface GarbageCollector {
        /**
         * garbage collecting a specific ledger
         *
         * @param ledgerId
         *          Ledger ID to be garbage collected
         */
        public void gc(long ledgerId);
    }

    /**
     * Garbage collecting all inactive/deleted ledgers
     * <p>
     * GarbageCollector#gc is triggered each time we found a ledger could be garbage collected.
     * After method finished, all those inactive ledgers should be garbage collected.
     * </p>
     *
     * @param gc garbage collector
     */
    public void garbageCollectLedgers(GarbageCollector gc);

    /**
     * Close ledger manager
     */
    public void close();
}
