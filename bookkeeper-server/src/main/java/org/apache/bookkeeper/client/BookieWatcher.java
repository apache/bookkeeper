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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.BKInterruptedException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BKException.MetaStoreException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * This class is responsible for maintaining a consistent view of what bookies
 * are available by reading Zookeeper (and setting watches on the bookie nodes).
 * When a bookie fails, the other parts of the code turn to this class to find a
 * replacement
 *
 */
@Slf4j
class BookieWatcher {

    private static final Function<Throwable, BKException> EXCEPTION_FUNC = cause -> {
        if (cause instanceof BKException) {
            log.error("Failed to get bookie list : ", cause);
            return (BKException) cause;
        } else if (cause instanceof InterruptedException) {
            log.error("Interrupted reading bookie list : ", cause);
            return new BKInterruptedException();
        } else {
            return new MetaStoreException();
        }
    };

    private final ClientConfiguration conf;
    private final RegistrationClient registrationClient;
    private final EnsemblePlacementPolicy placementPolicy;

    // Bookies that will not be preferred to be chosen in a new ensemble
    final Cache<BookieSocketAddress, Boolean> quarantinedBookies;

    private volatile Set<BookieSocketAddress> writableBookies = Collections.emptySet();
    private volatile Set<BookieSocketAddress> readOnlyBookies = Collections.emptySet();

    private CompletableFuture<?> initialWritableBookiesFuture = null;
    private CompletableFuture<?> initialReadonlyBookiesFuture = null;

    public BookieWatcher(ClientConfiguration conf,
                         EnsemblePlacementPolicy placementPolicy,
                         RegistrationClient registrationClient) {
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
    }

    public Set<BookieSocketAddress> getBookies() throws BKException {
        try {
            return FutureUtils.result(registrationClient.getWritableBookies(), EXCEPTION_FUNC).getValue();
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }

    public Set<BookieSocketAddress> getReadOnlyBookies() throws BKException {
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

    /**
     * Create an ensemble with given <i>ensembleSize</i> and <i>writeQuorumSize</i>.
     *
     * @param ensembleSize
     *          Ensemble Size
     * @param writeQuorumSize
     *          Write Quorum Size
     * @return list of bookies for new ensemble.
     * @throws BKNotEnoughBookiesException
     */
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize,
        int ackQuorumSize, Map<String, byte[]> customMetadata)
            throws BKNotEnoughBookiesException {
        try {
            // we try to only get from the healthy bookies first
            return placementPolicy.newEnsemble(ensembleSize,
                    writeQuorumSize, ackQuorumSize, customMetadata, new HashSet<BookieSocketAddress>(
                    quarantinedBookies.asMap().keySet()));
        } catch (BKNotEnoughBookiesException e) {
            if (log.isDebugEnabled()) {
                log.debug("Not enough healthy bookies available, using quarantined bookies");
            }
            return placementPolicy.newEnsemble(
                ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, Collections.emptySet());
        }
    }

    /**
     * Choose a bookie to replace bookie <i>bookieIdx</i> in <i>existingBookies</i>.
     * @param existingBookies
     *          list of existing bookies.
     * @param bookieIdx
     *          index of the bookie in the list to be replaced.
     * @return the bookie to replace.
     * @throws BKNotEnoughBookiesException
     */
    public BookieSocketAddress replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                             Map<String, byte[]> customMetadata,
                                             List<BookieSocketAddress> existingBookies, int bookieIdx,
                                             Set<BookieSocketAddress> excludeBookies)
            throws BKNotEnoughBookiesException {
        BookieSocketAddress addr = existingBookies.get(bookieIdx);
        try {
            // we exclude the quarantined bookies also first
            Set<BookieSocketAddress> existingAndQuarantinedBookies = new HashSet<BookieSocketAddress>(existingBookies);
            existingAndQuarantinedBookies.addAll(quarantinedBookies.asMap().keySet());
            return placementPolicy.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata,
                    existingAndQuarantinedBookies, addr, excludeBookies);
        } catch (BKNotEnoughBookiesException e) {
            if (log.isDebugEnabled()) {
                log.debug("Not enough healthy bookies available, using quarantined bookies");
            }
            return placementPolicy.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata,
                    new HashSet<BookieSocketAddress>(existingBookies), addr, excludeBookies);
        }
    }

    /**
     * Quarantine <i>bookie</i> so it will not be preferred to be chosen for new ensembles.
     * @param bookie
     * @return
     */
    public void quarantineBookie(BookieSocketAddress bookie) {
        if (quarantinedBookies.getIfPresent(bookie) == null) {
            quarantinedBookies.put(bookie, Boolean.TRUE);
            log.warn("Bookie {} has been quarantined because of read/write errors.", bookie);
        }
    }

}
