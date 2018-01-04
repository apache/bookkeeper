/*
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

package org.apache.bookkeeper.discover;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.LimitedPrivate;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerLayout;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.ZooKeeper;

/**
 * A registration client, which the bookkeeper client will use to interact with registration service.
 */
@LimitedPrivate
@Evolving
public interface RegistrationClient extends AutoCloseable {

    /**
     * Listener to receive changes from the registration service.
     */
    interface RegistrationListener {

        void onBookiesChanged(Versioned<Set<BookieSocketAddress>> bookies);

    }

    /**
     * Initialize the registration client with provided resources.
     *
     * <p>The existence of <i>zkSupplier</i> is for backward compatability.
     *
     * @param conf client configuration
     * @param statsLogger stats logger
     * @param zkOptional a supplier to supply zookeeper client.
     * @return
     */
    RegistrationClient initialize(ClientConfiguration conf,
                                  ScheduledExecutorService scheduler,
                                  StatsLogger statsLogger,
                                  Optional<ZooKeeper> zkOptional)
        throws BKException;

    @Override
    void close();

    /**
     * Get the list of writable bookie identifiers.
     *
     * @return a future represents the list of writable bookies.
     */
    CompletableFuture<Versioned<Set<BookieSocketAddress>>> getWritableBookies();

    /**
     * Get the list of readonly bookie identifiers.
     *
     * @return a future represents the list of readonly bookies.
     */
    CompletableFuture<Versioned<Set<BookieSocketAddress>>> getReadOnlyBookies();

    /**
     * Watch the changes of bookies.
     *
     * <p>The topology changes of bookies will be propagated to the provided <i>listener</i>.
     *
     * @param listener listener to receive the topology changes of bookies.
     * @return a future which completes when the bookies have been read for
     *         the first time
     */
    CompletableFuture<Void> watchWritableBookies(RegistrationListener listener);

    /**
     * Unwatch the changes of bookies.
     *
     * @param listener listener to receive the topology changes of bookies.
     */
    void unwatchWritableBookies(RegistrationListener listener);

    /**
     * Watch the changes of bookies.
     *
     * <p>The topology changes of bookies will be propagated to the provided <i>listener</i>.
     *
     * @param listener listener to receive the topology changes of bookies.
     * @return a future which completes when the bookies have been read for
     *         the first time
     */
    CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener listener);

    /**
     * Unwatch the changes of bookies.
     *
     * @param listener listener to receive the topology changes of bookies.
     */
    void unwatchReadOnlyBookies(RegistrationListener listener);

    /**
     * Gets layout manager.
     *
     * @return the layout manager
     */
    LayoutManager getLayoutManager();

    /**
     * Read ledger layout ledger layout.
     *
     * @return the ledger layout
     * @throws IOException the io exception
     */
    LedgerLayout readLedgerLayout() throws IOException;

    /**
     * Store ledger layout.
     *
     * @param layout the layout
     * @throws IOException the io exception
     */
    void storeLedgerLayout(LedgerLayout layout) throws IOException;

    /**
     * Delete ledger layout.
     *
     * @throws IOException the io exception
     */
    void deleteLedgerLayout() throws IOException;
}
