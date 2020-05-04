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
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNTER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.NEW_ENSEMBLE_TIME;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.REPLACE_BOOKIE_TIME;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WATCHER_SCOPE;
import static org.apache.bookkeeper.client.BookKeeperClientStats.CREATE_OP;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.bookie.BookKeeperServerStats;
import org.apache.bookkeeper.client.BKException.BKInterruptedException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BKException.MetaStoreException;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementPolicyAdherence;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * This class is responsible for maintaining a consistent view of what bookies
 * are available by reading Zookeeper (and setting watches on the bookie nodes).
 * When a bookie fails, the other parts of the code turn to this class to find a
 * replacement
 *
 */
@StatsDoc(
    name = WATCHER_SCOPE,
    help = "Bookie watcher related stats"
)
@Slf4j
class BookieWatcherImpl implements BookieWatcher {

    private static final Function<Throwable, BKException> EXCEPTION_FUNC = cause -> {
        if (cause instanceof BKException) {
            log.error("Failed to get bookie list : ", cause);
            return (BKException) cause;
        } else if (cause instanceof InterruptedException) {
            log.error("Interrupted reading bookie list : ", cause);
            return new BKInterruptedException();
        } else {
            MetaStoreException mse = new MetaStoreException(cause);
            return mse;
        }
    };

    private final ClientConfiguration conf;
    private final RegistrationClient registrationClient;
    private final EnsemblePlacementPolicy placementPolicy;
    @StatsDoc(
        name = NEW_ENSEMBLE_TIME,
        help = "operation stats of new ensembles",
        parent = CREATE_OP
    )
    private final OpStatsLogger newEnsembleTimer;
    @StatsDoc(
        name = REPLACE_BOOKIE_TIME,
        help = "operation stats of replacing bookie in an ensemble"
    )
    private final OpStatsLogger replaceBookieTimer;
    @StatsDoc(
            name = ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNTER,
            help = "total number of newEnsemble/replaceBookie operations failed to adhere"
            + " EnsemblePlacementPolicy"
    )
    private final Counter ensembleNotAdheringToPlacementPolicy;

    // Bookies that will not be preferred to be chosen in a new ensemble
    final Cache<BookieSocketAddress, Boolean> quarantinedBookies;

    private volatile Set<BookieSocketAddress> writableBookies = Collections.emptySet();
    private volatile Set<BookieSocketAddress> readOnlyBookies = Collections.emptySet();

    private CompletableFuture<?> initialWritableBookiesFuture = null;
    private CompletableFuture<?> initialReadonlyBookiesFuture = null;

    public BookieWatcherImpl(ClientConfiguration conf,
                             EnsemblePlacementPolicy placementPolicy,
                             RegistrationClient registrationClient,
                             StatsLogger statsLogger)  {
        this.conf = conf;
        this.placementPolicy = placementPolicy;
        this.registrationClient = registrationClient;
        this.quarantinedBookies = CacheBuilder.newBuilder()
                .expireAfterWrite(conf.getBookieQuarantineTimeSeconds(), TimeUnit.SECONDS)
                .removalListener(new RemovalListener<BookieSocketAddress, Boolean>() {

                    @Override
                    public void onRemoval(RemovalNotification<BookieSocketAddress, Boolean> bookie) {
                        log.info("Bookie {} is no longer quarantined", bookie.getKey());
                    }

                }).build();
        this.newEnsembleTimer = statsLogger.getOpStatsLogger(NEW_ENSEMBLE_TIME);
        this.replaceBookieTimer = statsLogger.getOpStatsLogger(REPLACE_BOOKIE_TIME);
        this.ensembleNotAdheringToPlacementPolicy = statsLogger
                .getCounter(BookKeeperServerStats.ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNTER);
    }

    @Override
    public Set<BookieSocketAddress> getBookies() throws BKException {
        try {
            return FutureUtils.result(registrationClient.getWritableBookies(), EXCEPTION_FUNC).getValue();
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }

    @Override
    public Set<BookieSocketAddress> getAllBookies() throws BKException {
        try {
            return FutureUtils.result(registrationClient.getAllBookies(), EXCEPTION_FUNC).getValue();
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }

    @Override
    public Set<BookieSocketAddress> getReadOnlyBookies()
            throws BKException {
        try {
            return FutureUtils.result(registrationClient.getReadOnlyBookies(), EXCEPTION_FUNC).getValue();
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }

    // this callback is already not executed in zookeeper thread
    private synchronized void processWritableBookiesChanged(Set<BookieSocketAddress> newBookieAddrs) {
        // Update watcher outside ZK callback thread, to avoid deadlock in case some other
        // component is trying to do a blocking ZK operation
        this.writableBookies = newBookieAddrs;
        placementPolicy.onClusterChanged(newBookieAddrs, readOnlyBookies);
        // we don't need to close clients here, because:
        // a. the dead bookies will be removed from topology, which will not be used in new ensemble.
        // b. the read sequence will be reordered based on znode availability, so most of the reads
        //    will not be sent to them.
        // c. the close here is just to disconnect the channel, which doesn't remove the channel from
        //    from pcbc map. we don't really need to disconnect the channel here, since if a bookie is
        //    really down, PCBC will disconnect itself based on netty callback. if we try to disconnect
        //    here, it actually introduces side-effects on case d.
        // d. closing the client here will affect latency if the bookie is alive but just being flaky
        //    on its znode registration due zookeeper session expire.
        // e. if we want to permanently remove a bookkeeper client, we should watch on the cookies' list.
        // if (bk.getBookieClient() != null) {
        //     bk.getBookieClient().closeClients(deadBookies);
        // }
    }

    private synchronized void processReadOnlyBookiesChanged(Set<BookieSocketAddress> readOnlyBookies) {
        this.readOnlyBookies = readOnlyBookies;
        placementPolicy.onClusterChanged(writableBookies, readOnlyBookies);
    }

    /**
     * Blocks until bookies are read from zookeeper, used in the {@link BookKeeper} constructor.
     *
     * @throws BKException when failed to read bookies
     */
    public void initialBlockingBookieRead() throws BKException {
        CompletableFuture<?> writable;
        CompletableFuture<?> readonly;
        synchronized (this) {
            if (initialReadonlyBookiesFuture == null) {
                assert initialWritableBookiesFuture == null;

                writable = this.registrationClient.watchWritableBookies(
                            bookies -> processWritableBookiesChanged(bookies.getValue()));

                readonly = this.registrationClient.watchReadOnlyBookies(
                            bookies -> processReadOnlyBookiesChanged(bookies.getValue()));
                initialWritableBookiesFuture = writable;
                initialReadonlyBookiesFuture = readonly;
            } else {
                writable = initialWritableBookiesFuture;
                readonly = initialReadonlyBookiesFuture;
            }
        }

        try {
            FutureUtils.result(writable, EXCEPTION_FUNC);
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
        try {
            FutureUtils.result(readonly, EXCEPTION_FUNC);
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        } catch (Exception e) {
            log.error("Failed getReadOnlyBookies: ", e);
        }
    }

    @Override
    public List<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize,
        int ackQuorumSize, Map<String, byte[]> customMetadata)
            throws BKNotEnoughBookiesException {
        long startTime = MathUtils.nowInNano();
        EnsemblePlacementPolicy.PlacementResult<List<BookieSocketAddress>> newEnsembleResponse;
        List<BookieSocketAddress> socketAddresses;
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy;
        try {
            Set<BookieSocketAddress> quarantinedBookiesSet = quarantinedBookies.asMap().keySet();
            newEnsembleResponse = placementPolicy.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize,
                    customMetadata, new HashSet<BookieSocketAddress>(quarantinedBookiesSet));
            socketAddresses = newEnsembleResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = newEnsembleResponse.isAdheringToPolicy();
            if (isEnsembleAdheringToPlacementPolicy == PlacementPolicyAdherence.FAIL) {
                ensembleNotAdheringToPlacementPolicy.inc();
                if (ensembleSize > 1) {
                    log.warn("New ensemble: {} is not adhering to Placement Policy. quarantinedBookies: {}",
                            socketAddresses, quarantinedBookiesSet);
                }
            }
            // we try to only get from the healthy bookies first
            newEnsembleTimer.registerSuccessfulEvent(MathUtils.nowInNano() - startTime, TimeUnit.NANOSECONDS);
        } catch (BKNotEnoughBookiesException e) {
            if (log.isDebugEnabled()) {
                log.debug("Not enough healthy bookies available, using quarantined bookies");
            }
            newEnsembleResponse = placementPolicy.newEnsemble(
                    ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, new HashSet<>());
            socketAddresses = newEnsembleResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = newEnsembleResponse.isAdheringToPolicy();
            if (isEnsembleAdheringToPlacementPolicy == PlacementPolicyAdherence.FAIL) {
                ensembleNotAdheringToPlacementPolicy.inc();
                log.warn("New ensemble: {} is not adhering to Placement Policy", socketAddresses);
            }
            newEnsembleTimer.registerFailedEvent(MathUtils.nowInNano() - startTime, TimeUnit.NANOSECONDS);
        }
        return socketAddresses;
    }

    @Override
    public BookieSocketAddress replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                             Map<String, byte[]> customMetadata,
                                             List<BookieSocketAddress> existingBookies, int bookieIdx,
                                             Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        long startTime = MathUtils.nowInNano();
        BookieSocketAddress addr = existingBookies.get(bookieIdx);
        EnsemblePlacementPolicy.PlacementResult<BookieSocketAddress> replaceBookieResponse;
        BookieSocketAddress socketAddress;
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy = PlacementPolicyAdherence.FAIL;
        try {
            // we exclude the quarantined bookies also first
            Set<BookieSocketAddress> excludedBookiesAndQuarantinedBookies = new HashSet<BookieSocketAddress>(
                    excludeBookies);
            Set<BookieSocketAddress> quarantinedBookiesSet = quarantinedBookies.asMap().keySet();
            excludedBookiesAndQuarantinedBookies.addAll(quarantinedBookiesSet);
            replaceBookieResponse = placementPolicy.replaceBookie(
                    ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata,
                    existingBookies, addr, excludedBookiesAndQuarantinedBookies);
            socketAddress = replaceBookieResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.isAdheringToPolicy();
            if (isEnsembleAdheringToPlacementPolicy == PlacementPolicyAdherence.FAIL) {
                ensembleNotAdheringToPlacementPolicy.inc();
                log.warn(
                        "replaceBookie for bookie: {} in ensemble: {} is not adhering to placement policy and"
                                + " chose {}. excludedBookies {} and quarantinedBookies {}",
                        addr, existingBookies, socketAddress, excludeBookies, quarantinedBookiesSet);
            }
            replaceBookieTimer.registerSuccessfulEvent(MathUtils.nowInNano() - startTime, TimeUnit.NANOSECONDS);
        } catch (BKNotEnoughBookiesException e) {
            if (log.isDebugEnabled()) {
                log.debug("Not enough healthy bookies available, using quarantined bookies");
            }
            replaceBookieResponse = placementPolicy.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize,
                    customMetadata, existingBookies, addr, excludeBookies);
            socketAddress = replaceBookieResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.isAdheringToPolicy();
            if (isEnsembleAdheringToPlacementPolicy == PlacementPolicyAdherence.FAIL) {
                ensembleNotAdheringToPlacementPolicy.inc();
                log.warn(
                        "replaceBookie for bookie: {} in ensemble: {} is not adhering to placement policy and"
                                + " chose {}. excludedBookies {}",
                        addr, existingBookies, socketAddress, excludeBookies);
            }
            replaceBookieTimer.registerFailedEvent(MathUtils.nowInNano() - startTime, TimeUnit.NANOSECONDS);
        }
        return socketAddress;
    }

    /**
     * Quarantine <i>bookie</i> so it will not be preferred to be chosen for new ensembles.
     * @param bookie
     */
    @Override
    public void quarantineBookie(BookieSocketAddress bookie) {
        if (quarantinedBookies.getIfPresent(bookie) == null) {
            quarantinedBookies.put(bookie, Boolean.TRUE);
            log.warn("Bookie {} has been quarantined because of read/write errors.", bookie);
        }
    }

}
