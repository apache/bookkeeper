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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.configuration.ConfigurationException;
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
public class ScanAndCompareGarbageCollector implements GarbageCollector {

    static final Logger LOG = LoggerFactory.getLogger(ScanAndCompareGarbageCollector.class);

    private final LedgerManager ledgerManager;
    private final CompactableLedgerStorage ledgerStorage;
    private final ServerConfiguration conf;
    private final BookieId selfBookieAddress;
    private boolean enableGcOverReplicatedLedger;
    private final long gcOverReplicatedLedgerIntervalMillis;
    private long lastOverReplicatedLedgerGcTimeMillis;
    private final boolean verifyMetadataOnGc;
    private int activeLedgerCounter;
    private StatsLogger statsLogger;
    private final int maxConcurrentRequests;

    public ScanAndCompareGarbageCollector(LedgerManager ledgerManager, CompactableLedgerStorage ledgerStorage,
            ServerConfiguration conf, StatsLogger statsLogger) throws IOException {
        this.ledgerManager = ledgerManager;
        this.ledgerStorage = ledgerStorage;
        this.conf = conf;
        this.statsLogger = statsLogger;
        this.selfBookieAddress = BookieImpl.getBookieId(conf);

        this.gcOverReplicatedLedgerIntervalMillis = conf.getGcOverreplicatedLedgerWaitTimeMillis();
        this.lastOverReplicatedLedgerGcTimeMillis = System.currentTimeMillis();
        if (gcOverReplicatedLedgerIntervalMillis > 0) {
            this.enableGcOverReplicatedLedger = true;
        }
        this.maxConcurrentRequests = conf.getGcOverreplicatedLedgerMaxConcurrentRequests();
        LOG.info("Over Replicated Ledger Deletion : enabled={}, interval={}, maxConcurrentRequests={}",
                enableGcOverReplicatedLedger, gcOverReplicatedLedgerIntervalMillis, maxConcurrentRequests);

        verifyMetadataOnGc = conf.getVerifyMetadataOnGC();

        this.activeLedgerCounter = 0;
    }

    public int getNumActiveLedgers() {
        return activeLedgerCounter;
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
            this.activeLedgerCounter = bkActiveLedgers.size();

            long curTime = System.currentTimeMillis();
            boolean checkOverreplicatedLedgers = (enableGcOverReplicatedLedger && curTime
                    - lastOverReplicatedLedgerGcTimeMillis > gcOverReplicatedLedgerIntervalMillis);
            if (checkOverreplicatedLedgers) {
                LOG.info("Start removing over-replicated ledgers. activeLedgerCounter={}", activeLedgerCounter);

                // remove all the overreplicated ledgers from the local bookie
                Set<Long> overReplicatedLedgers = removeOverReplicatedledgers(bkActiveLedgers, garbageCleaner);
                if (overReplicatedLedgers.isEmpty()) {
                    LOG.info("No over-replicated ledgers found.");
                } else {
                    LOG.info("Removed over-replicated ledgers: {}", overReplicatedLedgers);
                }
                lastOverReplicatedLedgerGcTimeMillis = System.currentTimeMillis();
            }

            // Iterate over all the ledger on the metadata store
            long zkOpTimeoutMs = this.conf.getZkTimeout() * 2;
            LedgerRangeIterator ledgerRangeIterator = ledgerManager
                    .getLedgerRanges(zkOpTimeoutMs);
            Set<Long> ledgersInMetadata = null;
            long start;
            long end = -1;
            boolean done = false;
            AtomicBoolean isBookieInEnsembles = new AtomicBoolean(false);
            Versioned<LedgerMetadata> metadata = null;
            while (!done) {
                start = end + 1;
                if (ledgerRangeIterator.hasNext()) {
                    LedgerRange lRange = ledgerRangeIterator.next();
                    ledgersInMetadata = lRange.getLedgers();
                    end = lRange.end();
                } else {
                    ledgersInMetadata = new TreeSet<>();
                    end = Long.MAX_VALUE;
                    done = true;
                }

                Iterable<Long> subBkActiveLedgers = bkActiveLedgers.subSet(start, true, end, true);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Active in metadata {}, Active in bookie {}", ledgersInMetadata, subBkActiveLedgers);
                }
                for (Long bkLid : subBkActiveLedgers) {
                    if (!ledgersInMetadata.contains(bkLid)) {
                        if (verifyMetadataOnGc) {
                            isBookieInEnsembles.set(false);
                            metadata = null;
                            int rc = BKException.Code.OK;
                            try {
                                metadata = result(ledgerManager.readLedgerMetadata(bkLid), zkOpTimeoutMs,
                                        TimeUnit.MILLISECONDS);
                            } catch (BKException | TimeoutException e) {
                                if (e instanceof BKException) {
                                    rc = ((BKException) e).getCode();
                                } else {
                                    LOG.warn("Time-out while fetching metadata for Ledger {} : {}.", bkLid,
                                            e.getMessage());

                                    continue;
                                }
                            }
                            // check bookie should be part of ensembles in one
                            // of the segment else ledger should be deleted from
                            // local storage
                            if (metadata != null && metadata.getValue() != null) {
                                metadata.getValue().getAllEnsembles().forEach((entryId, ensembles) -> {
                                    if (ensembles != null && ensembles.contains(selfBookieAddress)) {
                                        isBookieInEnsembles.set(true);
                                    }
                                });
                                if (isBookieInEnsembles.get()) {
                                    continue;
                                }
                            } else if (rc != BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                                LOG.warn("Ledger {} Missing in metadata list, but ledgerManager returned rc: {}.",
                                        bkLid, rc);
                                continue;
                            }
                        }
                        garbageCleaner.clean(bkLid);
                    }
                }
            }
        } catch (Throwable t) {
            // ignore exception, collecting garbage next time
            LOG.warn("Exception when iterating over the metadata", t);
        }
    }

    private Set<Long> removeOverReplicatedledgers(Set<Long> bkActiveledgers, final GarbageCleaner garbageCleaner)
            throws Exception {
        final Set<Long> overReplicatedLedgers = Sets.newHashSet();
        final Semaphore semaphore = new Semaphore(this.maxConcurrentRequests);
        final CountDownLatch latch = new CountDownLatch(bkActiveledgers.size());
        // instantiate zookeeper client to initialize ledger manager

        @Cleanup
        MetadataBookieDriver metadataDriver = instantiateMetadataDriver(conf, statsLogger);

        @Cleanup
        LedgerManagerFactory lmf = metadataDriver.getLedgerManagerFactory();

        @Cleanup
        LedgerUnderreplicationManager lum = lmf.newLedgerUnderreplicationManager();

        for (final Long ledgerId : bkActiveledgers) {
            try {
                // check ledger ensembles before creating lock nodes.
                // this is to reduce the number of lock node creations and deletions in ZK.
                // the ensemble check is done again after the lock node is created.
                Versioned<LedgerMetadata> preCheckMetadata = ledgerManager.readLedgerMetadata(ledgerId).get();
                if (!isNotBookieIncludedInLedgerEnsembles(preCheckMetadata)) {
                    latch.countDown();
                    continue;
                }
            } catch (Throwable t) {
                if (!(t.getCause() instanceof BKException.BKNoSuchLedgerExistsOnMetadataServerException)) {
                    LOG.warn("Failed to get metadata for ledger {}. {}: {}",
                            ledgerId, t.getClass().getName(), t.getMessage());
                }
                latch.countDown();
                continue;
            }

            try {
                // check if the ledger is being replicated already by the replication worker
                if (lum.isLedgerBeingReplicated(ledgerId)) {
                    latch.countDown();
                    continue;
                }
                // we try to acquire the underreplicated ledger lock to not let the bookie replicate the ledger that is
                // already being checked for deletion, since that might change the ledger ensemble to include the
                // current bookie again and, in that case, we cannot remove the ledger from local storage
                lum.acquireUnderreplicatedLedger(ledgerId);
                semaphore.acquire();
                ledgerManager.readLedgerMetadata(ledgerId)
                    .whenComplete((metadata, exception) -> {
                            try {
                                if (exception == null) {
                                    if (isNotBookieIncludedInLedgerEnsembles(metadata)) {
                                        // this bookie is not supposed to have this ledger,
                                        // thus we can delete this ledger now
                                        overReplicatedLedgers.add(ledgerId);
                                        garbageCleaner.clean(ledgerId);
                                    }
                                } else if (!(exception
                                        instanceof BKException.BKNoSuchLedgerExistsOnMetadataServerException)) {
                                    LOG.warn("Failed to get metadata for ledger {}. {}: {}",
                                            ledgerId, exception.getClass().getName(), exception.getMessage());
                                }
                            } finally {
                                semaphore.release();
                                latch.countDown();
                                try {
                                    lum.releaseUnderreplicatedLedger(ledgerId);
                                } catch (Throwable t) {
                                    LOG.error("Exception when removing underreplicated lock for ledger {}",
                                              ledgerId, t);
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

    private static MetadataBookieDriver instantiateMetadataDriver(ServerConfiguration conf, StatsLogger statsLogger)
            throws BookieException {
        try {
            String metadataServiceUriStr = conf.getMetadataServiceUri();
            MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(URI.create(metadataServiceUriStr));
            driver.initialize(
                    conf,
                    statsLogger);
            return driver;
        } catch (MetadataException me) {
            throw new BookieException.MetadataStoreException("Failed to initialize metadata bookie driver", me);
        } catch (ConfigurationException e) {
            throw new BookieException.BookieIllegalOpException(e);
        }
    }

    private boolean isNotBookieIncludedInLedgerEnsembles(Versioned<LedgerMetadata> metadata) {
        // do not delete a ledger that is not closed, since the ensemble might
        // change again and include the current bookie while we are deleting it
        if (!metadata.getValue().isClosed()) {
            return false;
        }

        SortedMap<Long, ? extends List<BookieId>> ensembles =
                metadata.getValue().getAllEnsembles();
        for (List<BookieId> ensemble : ensembles.values()) {
            // check if this bookie is supposed to have this ledger
            if (ensemble.contains(selfBookieAddress)) {
                return false;
            }
        }

        return true;
    }
}
