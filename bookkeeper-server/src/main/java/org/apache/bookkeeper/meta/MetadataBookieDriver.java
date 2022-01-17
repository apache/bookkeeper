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

import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Driver to manage all the metadata managers required by a bookie server.
 */
public interface MetadataBookieDriver extends AutoCloseable {

    /**
     * Initialize the metadata driver.
     *
     * @param conf configuration
     * @param statsLogger stats logger
     * @return metadata driver
     */
    MetadataBookieDriver initialize(ServerConfiguration conf,
                                    StatsLogger statsLogger)
        throws MetadataException;

    /**
     * Get the scheme of the metadata driver.
     *
     * @return the scheme of the metadata driver.
     */
    String getScheme();

    /**
     * Create the registration manager used for registering/unregistering bookies.
     *
     * @return the registration manager used for registering/unregistering bookies.
     */
    RegistrationManager createRegistrationManager();

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

    /**
     * Return health check is enable or disable.
     *
     * @return true if health check is enable, otherwise false.
     */
    default CompletableFuture<Boolean>  isHealthCheckEnabled() {
        return FutureUtils.value(true);
    }

    /**
     * Disable health check.
     */
    default CompletableFuture<Void> disableHealthCheck() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        result.completeExceptionally(new Exception("disableHealthCheck is not supported by this metadata driver"));
        return result;
    }

    /**
     * Enable health check.
     */
    default CompletableFuture<Void>  enableHealthCheck() {
        return FutureUtils.Void();
    }

    @Override
    void close();

}
