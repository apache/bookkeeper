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
package org.apache.bookkeeper.replication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.replication.ReplicationException.BKAuditException;
import org.apache.zookeeper.AsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Preparing bookie vs its corresponding ledgers. This will always look up the
 * ledgermanager for ledger metadata and will generate indexes.
 */
public class BookieLedgerIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(BookieLedgerIndexer.class);
    private final LedgerManager ledgerManager;

    public BookieLedgerIndexer(LedgerManager ledgerManager) {
        this.ledgerManager = ledgerManager;
    }

    /**
     * Generating bookie vs its ledgers map by reading all the ledgers in each
     * bookie and parsing its metadata.
     *
     * @return bookie2ledgersMap map of bookie vs ledgers
     * @throws BKAuditException
     *             exception while getting bookie-ledgers
     */
    public Map<String, Set<Long>> getBookieToLedgerIndex()
            throws BKAuditException {
        // bookie vs ledgers map
        final ConcurrentHashMap<String, Set<Long>> bookie2ledgersMap = new ConcurrentHashMap<String, Set<Long>>();
        final CountDownLatch ledgerCollectorLatch = new CountDownLatch(1);

        Processor<Long> ledgerProcessor = new Processor<Long>() {
                @Override
                public void process(Long ledgerId, AsyncCallback.VoidCallback iterCallback) {
                    ledgerManager.readLedgerMetadata(ledgerId).whenComplete(
                            (metadata, exception) -> {
                                if (exception == null) {
                                    for (Map.Entry<Long, ? extends List<BookieSocketAddress>> ensemble
                                             : metadata.getValue().getAllEnsembles().entrySet()) {
                                        for (BookieSocketAddress bookie : ensemble.getValue()) {
                                            putLedger(bookie2ledgersMap, bookie.toString(), ledgerId);
                                        }
                                    }
                                    iterCallback.processResult(BKException.Code.OK, null, null);
                                } else if (BKException.getExceptionCode(exception)
                                           == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                                    LOG.info("Ignoring replication of already deleted ledger {}", ledgerId);
                                    iterCallback.processResult(BKException.Code.OK, null, null);
                                } else {
                                    LOG.warn("Unable to read the ledger: {} information", ledgerId);
                                    iterCallback.processResult(BKException.getExceptionCode(exception), null, null);
                                }
                            });
                }
            };
        // Reading the result after processing all the ledgers
        final List<Integer> resultCode = new ArrayList<Integer>(1);
        ledgerManager.asyncProcessLedgers(ledgerProcessor,
                new AsyncCallback.VoidCallback() {

                    @Override
                    public void processResult(int rc, String s, Object obj) {
                        resultCode.add(rc);
                        ledgerCollectorLatch.countDown();
                    }
                }, null, BKException.Code.OK, BKException.Code.ReadException);
        try {
            ledgerCollectorLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BKAuditException(
                    "Exception while getting the bookie-ledgers", e);
        }
        if (!resultCode.contains(BKException.Code.OK)) {
            throw new BKAuditException(
                    "Exception while getting the bookie-ledgers", BKException
                            .create(resultCode.get(0)));
        }
        return bookie2ledgersMap;
    }

    private void putLedger(ConcurrentHashMap<String, Set<Long>> bookie2ledgersMap,
            String bookie, long ledgerId) {
        Set<Long> ledgers = bookie2ledgersMap.get(bookie);
        // creates an empty list and add to bookie for keeping its ledgers
        if (ledgers == null) {
            ledgers = Collections.synchronizedSet(new HashSet<Long>());
            Set<Long> oldLedgers = bookie2ledgersMap.putIfAbsent(bookie, ledgers);
            if (oldLedgers != null) {
                ledgers = oldLedgers;
            }
        }
        ledgers.add(ledgerId);
    }
}
