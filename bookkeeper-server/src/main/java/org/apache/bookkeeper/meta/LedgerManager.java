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
import java.io.IOException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.zookeeper.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.versioning.Version;

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
     *        {@link BKException.Code.ZKException} return code when can't generate
     *        or extract new ledger id
     */
    public void createLedger(LedgerMetadata metadata, GenericCallback<Long> cb);

    /**
     * Remove a specified ledger metadata by ledgerId and version.
     *
     * @param ledgerId
     *          Ledger Id
     * @param version
     *          Ledger metadata version
     * @param cb
     *          Callback when removed ledger metadata.
     *          {@link BKException.Code.MetadataVersionException} return code when version doesn't match,
     *          {@link BKException.Code.NoSuchLedgerExistsException} return code when ledger doesn't exist,
     *          {@link BKException.Code.ZKException} return code when other issues happen.
     */
    public void removeLedgerMetadata(long ledgerId, Version version, GenericCallback<Void> vb);

    /**
     * Read ledger metadata of a specified ledger.
     *
     * @param ledgerId
     *          Ledger Id
     * @param readCb
     *          Callback when read ledger metadata.
     *          {@link BKException.Code.NoSuchLedgerExistsException} return code when ledger doesn't exist,
     *          {@link BKException.Code.ZKException} return code when other issues happen.
     */
    public void readLedgerMetadata(long ledgerId, GenericCallback<LedgerMetadata> readCb);

    /**
     * Write ledger metadata.
     *
     * @param ledgerId
     *          Ledger Id
     * @param metadata
     *          Ledger Metadata to write
     * @param cb
     *          Callback when finished writing ledger metadata.
     *          {@link BKException.Code.MetadataVersionException} return code when version doesn't match,
     *          {@link BKException.Code.ZKException} return code when other issues happen.
     */
    public void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<Void> cb);

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
     * Loop to scan a range of metadata from metadata storage
     *
     * @return will return a iterator of the Ranges
     */
    public LedgerRangeIterator getLedgerRanges();

    /*
     * Used to represent the Ledgers range returned from the
     * current scan.
     */
    public static class LedgerRange {
        // returned ledgers
        private final SortedSet<Long> ledgers;

        public LedgerRange(Set<Long> ledgers) {
            this.ledgers = new TreeSet<Long>(ledgers);
        }

        public int size() {
            return this.ledgers.size();
        }

        public Long start() {
            return ledgers.first();
        }

        public Long end() {
            return ledgers.last();
        }

        public Set<Long> getLedgers() {
            return this.ledgers;
        }
    }

    /**
     * Interface of the ledger meta range iterator from
     * storage (e.g. in ZooKeeper or other key/value store)
     */
    interface LedgerRangeIterator {

        /**
         * @return true if there are records in the ledger metadata store. false
         * only when there are indeed no records in ledger metadata store.
         * @throws IOException thrown when there is any problem accessing the ledger
         * metadata store. It is critical that it doesn't return false in the case
         * in the case it fails to access the ledger metadata store. Otherwise it
         * will end up deleting all ledgers by accident.
         */
        public boolean hasNext() throws IOException;

        /**
         * Get the next element.
         *
         * @return the next element.
         * @throws IOException thrown when there is a problem accessing the ledger
         * metadata store. It is critical that it doesn't return false in the case
         * in the case it fails to access the ledger metadata store. Otherwise it
         * will end up deleting all ledgers by accident.
         */
        public LedgerRange next() throws IOException;
    }
}
