/*
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
 */
package org.apache.bookkeeper.meta;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.LimitedPrivate;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;

/**
 * Driver to manage all the metadata managers required by a bookkeeper client.
 */
@LimitedPrivate
@Evolving
public interface MetadataClientDriver extends AutoCloseable {

    /**
     * Initialize the metadata driver.
     *
     * @param conf configuration
     * @param scheduler scheduler
     * @param statsLogger stats logger
     * @param ctx optional context object passed in for initialization.
     *            currently it is an external zookeeper instance, which can
     *            be used for zookeeper based metadata implementation.
     * @return metadata driver
     * @throws MetadataException when fail to initialize the client driver.
     */
    MetadataClientDriver initialize(ClientConfiguration conf,
                                    ScheduledExecutorService scheduler,
                                    StatsLogger statsLogger,
                                    Optional<Object> ctx)
        throws MetadataException;

    /**
     * Get the scheme of the metadata driver.
     *
     * @return the scheme of the metadata driver.
     */
    String getScheme();

    /**
     * Return the registration client used for discovering registered bookies.
     *
     * @return the registration client used for discovering registered bookies.
     */
    RegistrationClient getRegistrationClient();

    /**
     * Return the ledger manager factory used for accessing ledger metadata.
     *
     * @return the ledger manager factory used for accessing ledger metadata.
     */
    LedgerManagerFactory getLedgerManagerFactory()
        throws MetadataException;

    /**
     * Return the layout manager.
     *
     * @return the layout manager.
     */
    LayoutManager getLayoutManager();

    @Override
    void close();

    /**
     * State Listener on listening the metadata client session states.
     */
    @FunctionalInterface
    interface SessionStateListener {

        /**
         * Signal when client session is expired.
         */
        void onSessionExpired();
    }

    /**
     * sets session state listener.
     *
     * @param sessionStateListener
     *            listener listening on metadata client session states.
     */
    void setSessionStateListener(SessionStateListener sessionStateListener);

    /**
     * Get the list of ledgers corresponding to the replica to be migrated.
     * */
    default List<String> listLedgersOfMigrationReplicas(String migrationReplicasPath)
            throws InterruptedException, KeeperException {
        return new ArrayList<>();
    }

    /**
     * After obtaining a replica migration task,
     * lock the replica task to prevent it from being executed by other workers.
     * */
    default void lockMigrationReplicas(String lockPath, String advertisedAddress)
            throws InterruptedException, KeeperException, UnsupportedOperationException{
        throw  new UnsupportedOperationException("lockMigrationReplicas is not supported "
                + "by this metadata driver");
    }

    /**
     * Get the bookies corresponding to the replica to be migrated.
     * */
    default String getOwnerBookiesMigrationReplicas(String ledgerForMigrationReplicasPath)
            throws InterruptedException, KeeperException, UnsupportedEncodingException {
        throw  new UnsupportedOperationException("getOwnerBookiesMigrationReplicas is not supported "
                + "by this metadata driver");
    }

    /**
     * Delete the corresponding zookeeper path.
     * */
    default void deleteZkPath(String path)
            throws InterruptedException, KeeperException, UnsupportedOperationException {
        throw  new UnsupportedOperationException("deleteZkPath is not supported "
                + "by this metadata driver");
    }

    /**
     * Check if zookeeper path exists.
     * */
    default boolean exists(String path) throws InterruptedException, KeeperException {
        throw  new UnsupportedOperationException("existPath is not supported "
                + "by this metadata driver");
    }

    /**
     * Return health check is enable or disable.
     *
     * @return true if health check is enable, otherwise false.
     */
    default CompletableFuture<Boolean> isHealthCheckEnabled() {
        return FutureUtils.value(true);
    }
}
