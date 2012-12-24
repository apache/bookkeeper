package org.apache.bookkeeper.meta;

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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public abstract class LedgerManagerFactory {

    static final Logger LOG = LoggerFactory.getLogger(LedgerManagerFactory.class);
    // v1 layout
    static final int V1 = 1;

    /**
     * Return current factory version.
     *
     * @return current version used by factory.
     */
    public abstract int getCurrentVersion();

    /**
     * Initialize a factory.
     *
     * @param conf
     *          Configuration object used to initialize factory
     * @param zk
     *          Available zookeeper handle for ledger manager to use.
     * @param factoryVersion
     *          What version used to initialize factory.
     * @return ledger manager factory instance
     * @throws IOException when fail to initialize the factory.
     */
    public abstract LedgerManagerFactory initialize(final AbstractConfiguration conf,
                                                    final ZooKeeper zk,
                                                    final int factoryVersion)
    throws IOException;

    /**
     * Uninitialize the factory.
     *
     * @throws IOException when fail to uninitialize the factory.
     */
    public abstract void uninitialize() throws IOException;

    /**
     * return ledger manager for client-side to manage ledger metadata.
     *
     * @return ledger manager
     * @see LedgerManager
     */
    public abstract LedgerManager newLedgerManager();

    /**
     * Return a ledger underreplication manager, which is used to
     * mark ledgers as unreplicated, and to retrieve a ledger which
     * is underreplicated so that it can be rereplicated.
     *
     * @return ledger underreplication manager
     * @see LedgerUnderreplicationManager
     */
    public abstract LedgerUnderreplicationManager newLedgerUnderreplicationManager()
            throws KeeperException, InterruptedException, ReplicationException.CompatibilityException;

    /**
     * Create new Ledger Manager Factory.
     *
     * @param conf
     *          Configuration Object.
     * @param zk
     *          ZooKeeper Client Handle, talk to zk to know which ledger manager is used.
     * @return new ledger manager factory
     * @throws IOException
     */
    public static LedgerManagerFactory newLedgerManagerFactory(
        final AbstractConfiguration conf, final ZooKeeper zk)
            throws IOException, KeeperException, InterruptedException {
        Class<? extends LedgerManagerFactory> factoryClass;
        try {
            factoryClass = conf.getLedgerManagerFactoryClass();
        } catch (Exception e) {
            throw new IOException("Failed to get ledger manager factory class from configuration : ", e);
        }
        String ledgerRootPath = conf.getZkLedgersRootPath();

        if (null == ledgerRootPath || ledgerRootPath.length() == 0) {
            throw new IOException("Empty Ledger Root Path.");
        }

        // if zk is null, return the default ledger manager
        if (zk == null) {
            return new FlatLedgerManagerFactory()
                   .initialize(conf, null, FlatLedgerManagerFactory.CUR_VERSION);
        }

        LedgerManagerFactory lmFactory;

        // check that the configured ledger manager is
        // compatible with the existing layout
        LedgerLayout layout = LedgerLayout.readLayout(zk, ledgerRootPath);
        if (layout == null) { // no existing layout
            lmFactory = createNewLMFactory(conf, zk, factoryClass);
            return lmFactory
                    .initialize(conf, zk, lmFactory.getCurrentVersion());
        }
        LOG.debug("read ledger layout {}", layout);

        // there is existing layout, we need to look into the layout.
        // handle pre V2 layout
        if (layout.getLayoutFormatVersion() <= V1) {
            // pre V2 layout we use type of ledger manager
            String lmType = conf.getLedgerManagerType();
            if (lmType != null && !layout.getManagerType().equals(lmType)) {
                throw new IOException("Configured layout " + lmType
                                    + " does not match existing layout "  + layout.getManagerType());
            }

            // create the ledger manager
            if (FlatLedgerManagerFactory.NAME.equals(layout.getManagerType())) {
                lmFactory = new FlatLedgerManagerFactory();
            } else if (HierarchicalLedgerManagerFactory.NAME.equals(layout.getManagerType())) {
                lmFactory = new HierarchicalLedgerManagerFactory();
            } else {
                throw new IOException("Unknown ledger manager type: " + lmType);
            }
            return lmFactory.initialize(conf, zk, layout.getManagerVersion());
        }

        // handle V2 layout case
        if (factoryClass != null &&
            !layout.getManagerFactoryClass().equals(factoryClass.getName())) {

            throw new IOException("Configured layout " + factoryClass.getName()
                                + " does not match existing layout "  + layout.getManagerFactoryClass());
        }
        if (factoryClass == null) {
            // no factory specified in configuration
            try {
                Class<?> theCls = Class.forName(layout.getManagerFactoryClass());
                if (!LedgerManagerFactory.class.isAssignableFrom(theCls)) {
                    throw new IOException("Wrong ledger manager factory " + layout.getManagerFactoryClass());
                }
                factoryClass = theCls.asSubclass(LedgerManagerFactory.class);
            } catch (ClassNotFoundException cnfe) {
                throw new IOException("Failed to instantiate ledger manager factory " + layout.getManagerFactoryClass());
            }
        }
        // instantiate a factory
        lmFactory = ReflectionUtils.newInstance(factoryClass);
        return lmFactory.initialize(conf, zk, layout.getManagerVersion());
    }

    /**
     * Creates the new layout and stores in zookeeper and returns the
     * LedgerManagerFactory instance.
     */
    private static LedgerManagerFactory createNewLMFactory(
            final AbstractConfiguration conf, final ZooKeeper zk,
            Class<? extends LedgerManagerFactory> factoryClass)
            throws IOException, KeeperException, InterruptedException {

        String ledgerRootPath = conf.getZkLedgersRootPath();
        LedgerManagerFactory lmFactory;
        LedgerLayout layout;
        // use default ledger manager factory if no one provided
        if (factoryClass == null) {
            // for backward compatibility, check manager type
            String lmType = conf.getLedgerManagerType();
            if (lmType == null) {
                factoryClass = FlatLedgerManagerFactory.class;
            } else {
                if (FlatLedgerManagerFactory.NAME.equals(lmType)) {
                    factoryClass = FlatLedgerManagerFactory.class;
                } else if (HierarchicalLedgerManagerFactory.NAME.equals(lmType)) {
                    factoryClass = HierarchicalLedgerManagerFactory.class;
                } else {
                    throw new IOException("Unknown ledger manager type: "
                            + lmType);
                }
            }
        }

        lmFactory = ReflectionUtils.newInstance(factoryClass);

        layout = new LedgerLayout(factoryClass.getName(),
                lmFactory.getCurrentVersion());
        try {
            layout.store(zk, ledgerRootPath);
        } catch (KeeperException.NodeExistsException nee) {
            LedgerLayout layout2 = LedgerLayout.readLayout(zk, ledgerRootPath);
            if (!layout2.equals(layout)) {
                throw new IOException(
                        "Contention writing to layout to zookeeper, "
                                + " other layout " + layout2
                                + " is incompatible with our " + "layout "
                                + layout);
            }
        }
        return lmFactory;
    }

    /**
     * Format the ledger metadata for LedgerManager
     * 
     * @param conf
     *            Configuration instance
     * @param zk
     *            Zookeeper instance
     */
    public void format(final AbstractConfiguration conf, final ZooKeeper zk)
            throws InterruptedException, KeeperException, IOException {
        
        Class<? extends LedgerManagerFactory> factoryClass;
        try {
            factoryClass = conf.getLedgerManagerFactoryClass();
        } catch (ConfigurationException e) {
            throw new IOException("Failed to get ledger manager factory class from configuration : ", e);
        }
       
        LedgerLayout layout = LedgerLayout.readLayout(zk,
                conf.getZkLedgersRootPath());
        layout.delete(zk, conf.getZkLedgersRootPath());
        // Create new layout information again.        
        createNewLMFactory(conf, zk, factoryClass);
    }
}
