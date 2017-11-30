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

package org.apache.bookkeeper.bookie;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Garbage collector implementation using scan and compare.
 *
 * <p>
 * Garbage collection is processed as below:
 * <ul>
 * <li> fetch all existing ledgers from zookeeper or metastore according to
 * the LedgerManager, called <b>globalActiveLedgers</b>
 * <li> fetch all active ledgers from bookie server, said <b>bkActiveLedgers</b>
 * <li> loop over <b>bkActiveLedgers</b> to find those ledgers that are not in
 * <b>globalActiveLedgers</b>, do garbage collection on them.
 * </ul>
 * </p>
 */
public class ScanAndCompareGarbageCollector implements GarbageCollector{

    static final Logger LOG = LoggerFactory.getLogger(ScanAndCompareGarbageCollector.class);
    static final int MAX_CONCURRENT_ZK_REQUESTS = 1000;

    private final LedgerManager ledgerManager;
    private final CompactableLedgerStorage ledgerStorage;
    private final ServerConfiguration conf;
    private final BookieSocketAddress selfBookieAddress;
    private ZooKeeper zk = null;
    private boolean enableGcOverReplicatedLedger;
    private final long gcOverReplicatedLedgerIntervalMillis;
    private long lastOverReplicatedLedgerGcTimeMillis;
    private final String zkLedgersRootPath;

    public ScanAndCompareGarbageCollector(LedgerManager ledgerManager, CompactableLedgerStorage ledgerStorage,
            ServerConfiguration conf) throws IOException {
        this.ledgerManager = ledgerManager;
        this.ledgerStorage = ledgerStorage;
        this.conf = conf;
        this.selfBookieAddress = Bookie.getBookieAddress(conf);
        this.gcOverReplicatedLedgerIntervalMillis = conf.getGcOverreplicatedLedgerWaitTimeMillis();
        this.lastOverReplicatedLedgerGcTimeMillis = MathUtils.now();
        if (gcOverReplicatedLedgerIntervalMillis > 0) {
            this.enableGcOverReplicatedLedger = true;
        }
        this.zkLedgersRootPath = conf.getZkLedgersRootPath();
        LOG.info("Over Replicated Ledger Deletion : enabled=" + enableGcOverReplicatedLedger + ", interval="
                + gcOverReplicatedLedgerIntervalMillis);
    }

    @Override
    public void gc(GarbageCleaner garbageCleaner) {
        if (null == ledgerManager) {
            // if ledger manager is null, the bookie is not started to connect to metadata store.
            // so skip garbage collection
            return;
        }

        try {
            // Get a set of all ledgers on the bookie
            NavigableSet<Long> bkActiveLedgers = Sets.newTreeSet(ledgerStorage.getActiveLedgersInRange(0,
                    Long.MAX_VALUE));

            // Iterate over all the ledger on the metadata store
            LedgerRangeIterator ledgerRangeIterator = ledgerManager.getLedgerRanges();

            if (!ledgerRangeIterator.hasNext()) {
                // Empty global active ledgers, need to remove all local active ledgers.
                for (long ledgerId : bkActiveLedgers) {
                    garbageCleaner.clean(ledgerId);
                }
            }

            long lastEnd = -1;

            long curTime = MathUtils.now();
            boolean checkOverreplicatedLedgers = (enableGcOverReplicatedLedger && curTime
                    - lastOverReplicatedLedgerGcTimeMillis > gcOverReplicatedLedgerIntervalMillis);
            if (checkOverreplicatedLedgers) {
                zk = ZooKeeperClient.newBuilder().connectString(conf.getZkServers())
                        .sessionTimeoutMs(conf.getZkTimeout()).build();
                // remove all the overreplicated ledgers from the local bookie
                Set<Long> overReplicatedLedgers = removeOverReplicatedledgers(bkActiveLedgers, garbageCleaner);
                if (overReplicatedLedgers.isEmpty()) {
                    LOG.info("No over-replicated ledgers found.");
                } else {
                    LOG.info("Removed over-replicated ledgers: {}", overReplicatedLedgers);
                }
                lastOverReplicatedLedgerGcTimeMillis = MathUtils.now();
            }

            while (ledgerRangeIterator.hasNext()) {
                LedgerRange lRange = ledgerRangeIterator.next();

                Long start = lastEnd + 1;
                Long end = lRange.end();
                if (!ledgerRangeIterator.hasNext()) {
                    end = Long.MAX_VALUE;
                }

                Iterable<Long> subBkActiveLedgers = bkActiveLedgers.subSet(start, true, end, true);

                Set<Long> ledgersInMetadata = lRange.getLedgers();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Active in metadata {}, Active in bookie {}", ledgersInMetadata, subBkActiveLedgers);
                }
                for (Long bkLid : subBkActiveLedgers) {
                    if (!ledgersInMetadata.contains(bkLid)) {
                        garbageCleaner.clean(bkLid);
                    }
                }
                lastEnd = end;
            }
        } catch (Throwable t) {
            // ignore exception, collecting garbage next time
            LOG.warn("Exception when iterating over the metadata {}", t);
        } finally {
            if (zk != null) {
                try {
                    zk.close();
                } catch (InterruptedException e) {
                    LOG.error("Error closing zk session", e);
                }
                zk = null;
            }
        }
    }

    private Set<Long> removeOverReplicatedledgers(Set<Long> bkActiveledgers, final GarbageCleaner garbageCleaner)
            throws InterruptedException, KeeperException {
        final List<ACL> zkAcls = ZkUtils.getACLs(conf);
        final Set<Long> overReplicatedLedgers = Sets.newHashSet();
        final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_ZK_REQUESTS);
        final CountDownLatch latch = new CountDownLatch(bkActiveledgers.size());
        for (final Long ledgerId : bkActiveledgers) {
            try {
                // check if the ledger is being replicated already by the replication worker
                if (ZkLedgerUnderreplicationManager.isLedgerBeingReplicated(zk, zkLedgersRootPath, ledgerId)) {
                    latch.countDown();
                    continue;
                }
                // we try to acquire the underreplicated ledger lock to not let the bookie replicate the ledger that is
                // already being checked for deletion, since that might change the ledger ensemble to include the
                // current bookie again and, in that case, we cannot remove the ledger from local storage
                ZkLedgerUnderreplicationManager.acquireUnderreplicatedLedgerLock(zk, zkLedgersRootPath, ledgerId,
                        zkAcls);
                semaphore.acquire();
                ledgerManager.readLedgerMetadata(ledgerId, new GenericCallback<LedgerMetadata>() {

                    @Override
                    public void operationComplete(int rc, LedgerMetadata ledgerMetadata) {
                        if (rc == BKException.Code.OK) {
                            // do not delete a ledger that is not closed, since the ensemble might change again and
                            // include the current bookie while we are deleting it
                            if (!ledgerMetadata.isClosed()) {
                                release();
                                return;
                            }
                            SortedMap<Long, ArrayList<BookieSocketAddress>> ensembles = ledgerMetadata.getEnsembles();
                            for (ArrayList<BookieSocketAddress> ensemble : ensembles.values()) {
                                // check if this bookie is supposed to have this ledger
                                if (ensemble.contains(selfBookieAddress)) {
                                    release();
                                    return;
                                }
                            }
                            // this bookie is not supposed to have this ledger, thus we can delete this ledger now
                            overReplicatedLedgers.add(ledgerId);
                            garbageCleaner.clean(ledgerId);
                        }
                        release();
                    }

                    private void release() {
                        semaphore.release();
                        latch.countDown();
                        try {
                            ZkLedgerUnderreplicationManager.releaseUnderreplicatedLedgerLock(zk, zkLedgersRootPath,
                                    ledgerId);
                        } catch (Throwable t) {
                            LOG.error("Exception when removing underreplicated lock for ledger {}", ledgerId, t);
                        }
                    }
                });
            } catch (Throwable t) {
                LOG.error("Exception when iterating through the ledgers to check for over-replication", t);
                latch.countDown();
            }
        }
        latch.await();
        bkActiveledgers.removeAll(overReplicatedLedgers);
        return overReplicatedLedgers;
    }
}
