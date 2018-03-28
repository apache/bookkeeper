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

import java.io.IOException;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.zookeeper.KeeperException;

/**
 * A factory for creating ledger managers.
 */
public interface LedgerManagerFactory extends AutoCloseable {

    /**
     * Return current factory version.
     *
     * @return current version used by factory.
     */
    int getCurrentVersion();

    /**
     * Initialize a factory.
     *
     * @param conf
     *          Configuration object used to initialize factory
     * @param layoutManager
     *          Layout manager used for initialize ledger manager factory
     * @param factoryVersion
     *          What version used to initialize factory.
     * @return ledger manager factory instance
     * @throws IOException when fail to initialize the factory.
     */
    LedgerManagerFactory initialize(AbstractConfiguration conf,
                                    LayoutManager layoutManager,
                                    int factoryVersion)
        throws IOException;

    /**
     * Uninitialize the factory.
     *
     * @throws IOException when fail to uninitialize the factory.
     */
    @Override
    void close() throws IOException;

    /**
     * Return the ledger id generator, which is used for global unique ledger id
     * generation.
     *
     * @return ledger id generator.
     */
    LedgerIdGenerator newLedgerIdGenerator();

    /**
     * return ledger manager for client-side to manage ledger metadata.
     *
     * @return ledger manager
     * @see LedgerManager
     */
    LedgerManager newLedgerManager();

    /**
     * Return a ledger underreplication manager, which is used to
     * mark ledgers as unreplicated, and to retrieve a ledger which
     * is underreplicated so that it can be rereplicated.
     *
     * @return ledger underreplication manager
     * @see LedgerUnderreplicationManager
     */
    LedgerUnderreplicationManager newLedgerUnderreplicationManager()
            throws KeeperException, InterruptedException, ReplicationException.CompatibilityException;

    /**
     * Format the ledger metadata for LedgerManager.
     *
     * @param conf
     *            Configuration instance
     * @param lm
     *            Layout manager
     */
    void format(AbstractConfiguration<?> conf, LayoutManager lm)
            throws InterruptedException, KeeperException, IOException;

    /**
     * This method makes sure there are no unexpected znodes under ledgersRootPath
     * and then it proceeds with ledger metadata formatting and nuking the cluster
     * ZK state info.
     *
     * @param conf
     *          Configuration instance
     * @param lm
     *          Layout manager
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    boolean validateAndNukeExistingCluster(AbstractConfiguration<?> conf, LayoutManager lm)
            throws InterruptedException, KeeperException, IOException;
}
