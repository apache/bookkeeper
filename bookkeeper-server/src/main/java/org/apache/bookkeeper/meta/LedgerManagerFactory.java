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
import org.apache.bookkeeper.meta.LayoutManager.LedgerLayoutExistsException;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory for creating ledger managers.
 */
public abstract class LedgerManagerFactory implements AutoCloseable {

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
     * @param layoutManager
     *          Layout manager used for initialize ledger manager factory
     * @param factoryVersion
     *          What version used to initialize factory.
     * @return ledger manager factory instance
     * @throws IOException when fail to initialize the factory.
     */
    public abstract LedgerManagerFactory initialize(AbstractConfiguration conf,
                                                    LayoutManager layoutManager,
                                                    int factoryVersion)
    throws IOException;

    /**
     * Uninitialize the factory.
     *
     * @throws IOException when fail to uninitialize the factory.
     */
    @Override
    public void close() throws IOException {
    }

    /**
     * Return the ledger id generator, which is used for global unique ledger id
     * generation.
     *
     * @return ledger id generator.
     */
    public abstract LedgerIdGenerator newLedgerIdGenerator();

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
     * @param layoutManager
     *          layout manager
     * @return new ledger manager factory
     * @throws IOException
     */
    public static LedgerManagerFactory newLedgerManagerFactory(
        final AbstractConfiguration<?> conf, LayoutManager layoutManager)
            throws IOException, InterruptedException {
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

        // if layoutManager is null, return the default ledger manager
        if (layoutManager == null) {
            return new FlatLedgerManagerFactory()
                   .initialize(conf, null, FlatLedgerManagerFactory.CUR_VERSION);
        }

        LedgerManagerFactory lmFactory;

        // check that the configured ledger manager is
        // compatible with the existing layout
        LedgerLayout layout = layoutManager.readLedgerLayout();

        if (layout == null) { // no existing layout
            lmFactory = createNewLMFactory(conf, layoutManager, factoryClass);
            return lmFactory
                    .initialize(conf, layoutManager, lmFactory.getCurrentVersion());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("read ledger layout {}", layout);
        }

        // there is existing layout, we need to look into the layout.
        // handle pre V2 layout
        if (layout.getLayoutFormatVersion() <= V1) {
            // pre V2 layout we use type of ledger manager
            @SuppressWarnings("deprecation")
            String lmType = conf.getLedgerManagerType();
            if (lmType != null && !layout.getManagerFactoryClass().equals(lmType)) {
                throw new IOException("Configured layout " + lmType
                        + " does not match existing layout "  + layout.getManagerFactoryClass());
            }

            // create the ledger manager
            if (FlatLedgerManagerFactory.NAME.equals(layout.getManagerFactoryClass())) {
                lmFactory = new FlatLedgerManagerFactory();
            } else if (HierarchicalLedgerManagerFactory.NAME.equals(layout.getManagerFactoryClass())) {
                lmFactory = new HierarchicalLedgerManagerFactory();
            } else {
                throw new IOException("Unknown ledger manager type: " + lmType);
            }
            return lmFactory.initialize(conf, layoutManager, layout.getManagerVersion());
        }

        // handle V2 layout case
        if (factoryClass != null && !layout.getManagerFactoryClass().equals(factoryClass.getName())
                && conf.getProperty(AbstractConfiguration.LEDGER_MANAGER_FACTORY_DISABLE_CLASS_CHECK) == null) {
                // Disable should ONLY happen during compatibility testing.

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
                throw new IOException("Failed to instantiate ledger manager factory "
                        + layout.getManagerFactoryClass());
            }
        }
        // instantiate a factory
        lmFactory = ReflectionUtils.newInstance(factoryClass);
        return lmFactory.initialize(conf, layoutManager, layout.getManagerVersion());
    }

    /**
     * Creates the new layout and stores in zookeeper and returns the
     * LedgerManagerFactory instance.
     */
    private static LedgerManagerFactory createNewLMFactory(
            final AbstractConfiguration conf, final LayoutManager layoutManager,
            Class<? extends LedgerManagerFactory> factoryClass)
            throws IOException, InterruptedException {

        LedgerManagerFactory lmFactory;
        LedgerLayout layout;
        // use default ledger manager factory if no one provided
        if (factoryClass == null) {
            // for backward compatibility, check manager type
            @SuppressWarnings("deprecation")
            String lmType = conf.getLedgerManagerType();
            if (lmType == null) {
                factoryClass = FlatLedgerManagerFactory.class;
            } else {
                if (FlatLedgerManagerFactory.NAME.equals(lmType)) {
                    factoryClass = FlatLedgerManagerFactory.class;
                } else if (HierarchicalLedgerManagerFactory.NAME.equals(lmType)) {
                    factoryClass = HierarchicalLedgerManagerFactory.class;
                } else if (LongHierarchicalLedgerManagerFactory.NAME.equals(lmType)) {
                    factoryClass = LongHierarchicalLedgerManagerFactory.class;
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
            layoutManager.storeLedgerLayout(layout);
        } catch (LedgerLayoutExistsException e) {
            LedgerLayout layout2 = layoutManager.readLedgerLayout();
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
     * Format the ledger metadata for LedgerManager.
     *
     * @param conf
     *            Configuration instance
     * @param lm
     *            Layout manager
     */
    public void format(final AbstractConfiguration<?> conf, final LayoutManager lm)
            throws InterruptedException, KeeperException, IOException {

        Class<? extends LedgerManagerFactory> factoryClass;
        try {
            factoryClass = conf.getLedgerManagerFactoryClass();
        } catch (ConfigurationException e) {
            throw new IOException("Failed to get ledger manager factory class from configuration : ", e);
        }

        lm.deleteLedgerLayout();
        // Create new layout information again.
        createNewLMFactory(conf, lm, factoryClass);
    }

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
    public abstract boolean validateAndNukeExistingCluster(AbstractConfiguration<?> conf, LayoutManager lm)
            throws InterruptedException, KeeperException, IOException;
}
