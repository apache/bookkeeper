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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNT;
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
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieAddressResolver;
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
            name = ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNT,
            help = "total number of newEnsemble/replaceBookie operations failed to adhere"
            + " EnsemblePlacementPolicy"
    )
    private final Counter ensembleNotAdheringToPlacementPolicy;

    // Bookies that will not be preferred to be chosen in a new ensemble
    final Cache<BookieId, Boolean> quarantinedBookies;

    private volatile Set<BookieId> writableBookies = Collections.emptySet();
    private volatile Set<BookieId> readOnlyBookies = Collections.emptySet();

    private CompletableFuture<?> initialWritableBookiesFuture = null;
    private CompletableFuture<?> initialReadonlyBookiesFuture = null;

    private final BookieAddressResolver bookieAddressResolver;

    public BookieWatcherImpl(ClientConfiguration conf,
                             EnsemblePlacementPolicy placementPolicy,
                             RegistrationClient registrationClient,
                             BookieAddressResolver bookieAddressResolver,
                             StatsLogger statsLogger)  {
        this.conf = conf;
        this.bookieAddressResolver = bookieAddressResolver;
        this.placementPolicy = placementPolicy;
        this.registrationClient = registrationClient;
        this.quarantinedBookies = CacheBuilder.newBuilder()
                .expireAfterWrite(conf.getBookieQuarantineTimeSeconds(), TimeUnit.SECONDS)
                .removalListener(new RemovalListener<BookieId, Boolean>() {

                    @Override
                    public void onRemoval(RemovalNotification<BookieId, Boolean> bookie) {
                        log.info("Bookie {} is no longer quarantined", bookie.getKey());
                    }

                }).build();
        this.newEnsembleTimer = statsLogger.getOpStatsLogger(NEW_ENSEMBLE_TIME);
        this.replaceBookieTimer = statsLogger.getOpStatsLogger(REPLACE_BOOKIE_TIME);
        this.ensembleNotAdheringToPlacementPolicy = statsLogger
                .getCounter(BookKeeperServerStats.ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNT);
    }

    @Override
    public Set<BookieId> getBookies() throws BKException {
        try {
            return FutureUtils.result(registrationClient.getWritableBookies(), EXCEPTION_FUNC).getValue();
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }

    @Override
    public Set<BookieId> getAllBookies() throws BKException {
        try {
            return FutureUtils.result(registrationClient.getAllBookies(), EXCEPTION_FUNC).getValue();
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }

    @Override
    public BookieAddressResolver getBookieAddressResolver() {
        return this.bookieAddressResolver;
    }

    @Override
    public Set<BookieId> getReadOnlyBookies()
            throws BKException {
        try {
            return FutureUtils.result(registrationClient.getReadOnlyBookies(), EXCEPTION_FUNC).getValue();
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }

    /**
     * Determine if a bookie should be considered unavailable.
     * This does not require a network call because this class
     * maintains a current view of readonly and writable bookies.
     * An unavailable bookie is one that is neither read only nor
     * writable.
     *
     * @param id
     *          Bookie to check
     * @return whether or not the given bookie is unavailable
     */
    @Override
    public boolean isBookieUnavailable(BookieId id) {
        return !readOnlyBookies.contains(id) && !writableBookies.contains(id);
    }

    // this callback is already not executed in zookeeper thread
    private synchronized void processWritableBookiesChanged(Set<BookieId> newBookieAddrs) {
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

    private synchronized void processReadOnlyBookiesChanged(Set<BookieId> readOnlyBookies) {
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
    public List<BookieId> newEnsemble(int ensembleSize, int writeQuorumSize,
        int ackQuorumSize, Map<String, byte[]> customMetadata)
            throws BKNotEnoughBookiesException {
        long startTime = MathUtils.nowInNano();
        EnsemblePlacementPolicy.PlacementResult<List<BookieId>> newEnsembleResponse;
        List<BookieId> socketAddresses;
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy;
        try {
            Set<BookieId> quarantinedBookiesSet = quarantinedBookies.asMap().keySet();
            newEnsembleResponse = placementPolicy.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize,
                    customMetadata, new HashSet<BookieId>(quarantinedBookiesSet));
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
    public BookieId replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                             Map<String, byte[]> customMetadata,
                                             List<BookieId> existingBookies, int bookieIdx,
                                             Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException {
        long startTime = MathUtils.nowInNano();
        BookieId addr = existingBookies.get(bookieIdx);
        EnsemblePlacementPolicy.PlacementResult<BookieId> replaceBookieResponse;
        BookieId socketAddress;
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy = PlacementPolicyAdherence.FAIL;
        try {
            // we exclude the quarantined bookies also first
            Set<BookieId> excludedBookiesAndQuarantinedBookies = new HashSet<BookieId>(
                    excludeBookies);
            Set<BookieId> quarantinedBookiesSet = quarantinedBookies.asMap().keySet();
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
    public void quarantineBookie(BookieId bookie) {
        if (quarantinedBookies.getIfPresent(bookie) == null) {
            quarantinedBookies.put(bookie, Boolean.TRUE);
            log.warn("Bookie {} has been quarantined because of read/write errors.", bookie);
        }
    }


}
