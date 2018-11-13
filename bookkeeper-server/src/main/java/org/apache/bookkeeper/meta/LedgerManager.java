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
package org.apache.bookkeeper.meta;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;

/**
 * LedgerManager takes responsibility of ledger management in client side.
 *
 * <ul>
 * <li>How to store ledger meta (e.g. in ZooKeeper or other key/value store)
 * </ul>
 */
public interface LedgerManager extends Closeable {

    /**
     * Create a new ledger with provided ledger id and metadata.
     *
     * @param ledgerId
     *            Ledger id provided to be created
     * @param metadata
     *            Metadata provided when creating the new ledger
     * @param cb
     *            Callback when creating a new ledger, returning the written metadata.
     *            Return code:<ul>
     *            <li>{@link BKException.Code.OK} if success</li>
     *            <li>{@link BKException.Code.LedgerExistException} if given ledger id exist</li>
     *            <li>{@link BKException.Code.ZKException}/{@link BKException.Code.MetaStoreException}
     *                 for other issue</li>
     *            </ul>
     */
    void createLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<Versioned<LedgerMetadata>> cb);

    /**
     * Remove a specified ledger metadata by ledgerId and version.
     *
     * @param ledgerId
     *          Ledger Id
     * @param version
     *          Ledger metadata version
     * @param cb
     *          Callback when remove ledger metadata. Return code:<ul>
     *          <li>{@link BKException.Code.OK} if success</li>
     *          <li>{@link BKException.Code.MetadataVersionException} if version doesn't match</li>
     *          <li>{@link BKException.Code.NoSuchLedgerExistsException} if ledger not exist</li>
     *          <li>{@link BKException.Code.ZKException} for other issue</li>
     *          </ul>
     */
    void removeLedgerMetadata(long ledgerId, Version version, GenericCallback<Void> cb);

    /**
     * Read ledger metadata of a specified ledger.
     *
     * @param ledgerId
     *          Ledger Id
     * @param readCb
     *          Callback when read ledger metadata. Return code:<ul>
     *          <li>{@link BKException.Code.OK} if success</li>
     *          <li>{@link BKException.Code.NoSuchLedgerExistsException} if ledger not exist</li>
     *          <li>{@link BKException.Code.ZKException} for other issue</li>
     *          </ul>
     */
    void readLedgerMetadata(long ledgerId, GenericCallback<Versioned<LedgerMetadata>> readCb);

    /**
     * Write ledger metadata.
     *
     * @param ledgerId
     *          Ledger Id
     * @param metadata
     *          Ledger Metadata to write
     * @param currentVersion
     *          The version of the metadata we expect to be overwriting.
     * @param cb
     *          Callback when finished writing ledger metadata, returning the written metadata.
     *          Return code:<ul>
     *          <li>{@link BKException.Code.OK} if success</li>
     *          <li>{@link BKException.Code.MetadataVersionException} if version in metadata doesn't match</li>
     *          <li>{@link BKException.Code.ZKException} for other issue</li>
     *          </ul>
     */
    void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata,
                             Version currentVersion,
                             GenericCallback<Versioned<LedgerMetadata>> cb);

    /**
     * Register the ledger metadata <i>listener</i> on <i>ledgerId</i>.
     *
     * @param ledgerId
     *          ledger id.
     * @param listener
     *          listener.
     */
    void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener);

    /**
     * Unregister the ledger metadata <i>listener</i> on <i>ledgerId</i>.
     *
     * @param ledgerId
     *          ledger id.
     * @param listener
     *          ledger metadata listener.
     */
    void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener);

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
    void asyncProcessLedgers(Processor<Long> processor, AsyncCallback.VoidCallback finalCb,
                                    Object context, int successRc, int failureRc);

    /**
     * Loop to scan a range of metadata from metadata storage.
     *
     * @return will return a iterator of the Ranges
     */
    LedgerRangeIterator getLedgerRanges();

    /**
     * Used to represent the Ledgers range returned from the
     * current scan.
     */
    class LedgerRange {
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
     * storage (e.g. in ZooKeeper or other key/value store).
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
        boolean hasNext() throws IOException;

        /**
         * Get the next element.
         *
         * @return the next element, the LedgerRange returned must be non-empty
         * @throws IOException thrown when there is a problem accessing the ledger
         * metadata store. It is critical that it doesn't return false in the case
         * in the case it fails to access the ledger metadata store. Otherwise it
         * will end up deleting all ledgers by accident.
         */
        LedgerRange next() throws IOException;
    }
}
