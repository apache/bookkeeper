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

import static org.apache.bookkeeper.meta.AbstractZkLedgerManager.isLeadgerIdGeneratorZnode;
import static org.apache.bookkeeper.meta.AbstractZkLedgerManager.isSpecialZnode;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.LayoutManager.LedgerLayoutExistsException;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;

/**
 * Abstract ledger manager factory based on zookeeper, which provides common
 * methods such as format and validateAndNukeExistingCluster.
 */
@Slf4j
public abstract class AbstractZkLedgerManagerFactory implements LedgerManagerFactory {

    protected ZooKeeper zk;

    @SuppressWarnings("deprecation")
    @Override
    public void format(AbstractConfiguration<?> conf, LayoutManager layoutManager)
            throws InterruptedException, KeeperException, IOException {
        try (AbstractZkLedgerManager ledgerManager = (AbstractZkLedgerManager) newLedgerManager()) {
            String ledgersRootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf);
            List<String> children = zk.getChildren(ledgersRootPath, false);
            for (String child : children) {
                boolean lParentNode = !isSpecialZnode(child) && ledgerManager.isLedgerParentNode(child);
                boolean lIdGenerator = isLeadgerIdGeneratorZnode(child);

                if (lParentNode || lIdGenerator) {
                    ZKUtil.deleteRecursive(zk, ledgersRootPath + "/" + child);
                }
            }
        }

        Class<? extends LedgerManagerFactory> factoryClass;
        try {
            factoryClass = conf.getLedgerManagerFactoryClass();
        } catch (ConfigurationException e) {
            throw new IOException("Failed to get ledger manager factory class from configuration : ", e);
        }

        layoutManager.deleteLedgerLayout();
        // Create new layout information again.
        createNewLMFactory(conf, layoutManager, factoryClass);
    }

    @Override
    public boolean validateAndNukeExistingCluster(AbstractConfiguration<?> conf, LayoutManager layoutManager)
            throws InterruptedException, KeeperException, IOException {
        String zkLedgersRootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf);
        String zkServers = ZKMetadataDriverBase.resolveZkServers(conf);
        AbstractZkLedgerManager zkLedgerManager = (AbstractZkLedgerManager) newLedgerManager();

        /*
         * before proceeding with nuking existing cluster, make sure there
         * are no unexpected znodes under ledgersRootPath
         */
        List<String> ledgersRootPathChildrenList = zk.getChildren(zkLedgersRootPath, false);
        for (String ledgersRootPathChildren : ledgersRootPathChildrenList) {
            if ((!AbstractZkLedgerManager.isSpecialZnode(ledgersRootPathChildren))
                    && (!zkLedgerManager.isLedgerParentNode(ledgersRootPathChildren))) {
                log.error("Found unexpected znode : {} under ledgersRootPath : {} so exiting nuke operation",
                        ledgersRootPathChildren, zkLedgersRootPath);
                return false;
            }
        }

        // formatting ledgermanager deletes ledger znodes
        format(conf, layoutManager);

        // now delete all the special nodes recursively
        ledgersRootPathChildrenList = zk.getChildren(zkLedgersRootPath, false);
        for (String ledgersRootPathChildren : ledgersRootPathChildrenList) {
            if (AbstractZkLedgerManager.isSpecialZnode(ledgersRootPathChildren)) {
                ZKUtil.deleteRecursive(zk, zkLedgersRootPath + "/" + ledgersRootPathChildren);
            } else {
                log.error("Found unexpected znode : {} under ledgersRootPath : {} so exiting nuke operation",
                        ledgersRootPathChildren, zkLedgersRootPath);
                return false;
            }
        }

        // finally deleting the ledgers rootpath
        zk.delete(zkLedgersRootPath, -1);

        log.info("Successfully nuked existing cluster, ZKServers: {} ledger root path: {}",
                zkServers, zkLedgersRootPath);
        return true;
    }

    // v1 layout
    static final int V1 = 1;

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
    @SuppressWarnings("deprecation")
    public static LedgerManagerFactory newLedgerManagerFactory(
        final AbstractConfiguration<?> conf, LayoutManager layoutManager)
            throws IOException, InterruptedException {
        String metadataServiceUriStr;
        try {
            metadataServiceUriStr = conf.getMetadataServiceUri();
        } catch (ConfigurationException e) {
            log.error("Failed to retrieve metadata service uri from configuration", e);
            throw new IOException(
                "Failed to retrieve metadata service uri from configuration", e);
        }

        Class<? extends LedgerManagerFactory> factoryClass;
        String ledgerRootPath;
        // `metadataServiceUri` can be null when constructing bookkeeper client using an external zookeeper client.
        if (null == metadataServiceUriStr) { //
            try {
                factoryClass = conf.getLedgerManagerFactoryClass();
            } catch (ConfigurationException e) {
                log.error("Failed to get ledger manager factory class when using an external zookeeper client", e);
                throw new IOException(
                    "Failed to get ledger manager factory class when using an external zookeeper client", e);
            }
            ledgerRootPath = conf.getZkLedgersRootPath();
        } else {
            URI metadataServiceUri = URI.create(metadataServiceUriStr);
            factoryClass = ZKMetadataDriverBase.resolveLedgerManagerFactory(metadataServiceUri);
            ledgerRootPath = metadataServiceUri.getPath();
        }

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
        if (log.isDebugEnabled()) {
            log.debug("read ledger layout {}", layout);
        }

        // there is existing layout, we need to look into the layout.
        // handle pre V2 layout
        if (layout.getLayoutFormatVersion() <= V1) {
            // pre V2 layout we use type of ledger manager
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
        if (factoryClass != null
                && !isSameLedgerManagerFactory(
                    conf, layout.getManagerFactoryClass(), factoryClass.getName())
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
            } catch (ClassNotFoundException | IOException e) {
                factoryClass = attemptToResolveShadedLedgerManagerFactory(
                    conf,
                    layout.getManagerFactoryClass(),
                    e);
            }
        }
        // instantiate a factory
        lmFactory = ReflectionUtils.newInstance(factoryClass);
        return lmFactory.initialize(conf, layoutManager, layout.getManagerVersion());
    }

    private static String normalizedLedgerManagerFactoryClassName(String factoryClass,
                                                                  String shadedClassPrefix,
                                                                  boolean isShadedClassAllowed) {
        if (isShadedClassAllowed) {
            if (null == factoryClass || null == shadedClassPrefix) {
                return factoryClass;
            } else {
                return factoryClass.replace(shadedClassPrefix, "");
            }
        } else {
            return factoryClass;
        }
    }

    private static boolean isSameLedgerManagerFactory(
            AbstractConfiguration<?> conf, String leftFactoryClass, String rightFactoryClass) {
        leftFactoryClass = normalizedLedgerManagerFactoryClassName(
            leftFactoryClass,
            conf.getShadedLedgerManagerFactoryClassPrefix(),
            conf.isShadedLedgerManagerFactoryClassAllowed());
        rightFactoryClass = normalizedLedgerManagerFactoryClassName(
            rightFactoryClass,
            conf.getShadedLedgerManagerFactoryClassPrefix(),
            conf.isShadedLedgerManagerFactoryClassAllowed());
        return Objects.equals(leftFactoryClass, rightFactoryClass);
    }

    private static Class<? extends LedgerManagerFactory> attemptToResolveShadedLedgerManagerFactory(
            AbstractConfiguration<?> conf, String lmfClassName, Throwable cause) throws IOException {
        if (conf.isShadedLedgerManagerFactoryClassAllowed()) {
            String shadedPrefix = conf.getShadedLedgerManagerFactoryClassPrefix();
            log.warn("Failed to instantiate ledger manager factory {}, trying its shaded class {}{}",
                lmfClassName, shadedPrefix, lmfClassName);
            // try shading class
            try {
                return resolveShadedLedgerManagerFactory(lmfClassName, shadedPrefix);
            } catch (ClassNotFoundException cnfe) {
                throw new IOException("Failed to instantiate ledger manager factory "
                    + lmfClassName + " and its shaded class " + shadedPrefix + lmfClassName, cnfe);
            }
        } else {
            throw new IOException("Failed to instantiate ledger manager factory "
                + lmfClassName, cause);
        }
    }

    private static Class<? extends LedgerManagerFactory> resolveShadedLedgerManagerFactory(String lmfClassName,
                                                                                           String shadedClassPrefix)
            throws ClassNotFoundException, IOException {
        if (null == lmfClassName) {
            return null;
        } else {
            // this is to address the issue when using the dlog shaded jar
            Class<?> theCls = Class.forName(shadedClassPrefix + lmfClassName);
            if (!LedgerManagerFactory.class.isAssignableFrom(theCls)) {
                throw new IOException("Wrong shaded ledger manager factory : " + shadedClassPrefix + lmfClassName);
            }
            return theCls.asSubclass(LedgerManagerFactory.class);
        }
    }

    /**
     * Creates the new layout and stores in zookeeper and returns the
     * LedgerManagerFactory instance.
     */
    @SuppressWarnings("deprecation")
    protected static LedgerManagerFactory createNewLMFactory(
            final AbstractConfiguration conf, final LayoutManager layoutManager,
            Class<? extends LedgerManagerFactory> factoryClass)
            throws IOException, InterruptedException {

        LedgerManagerFactory lmFactory;
        LedgerLayout layout;
        // use default ledger manager factory if no one provided
        if (factoryClass == null) {
            // for backward compatibility, check manager type
            String lmType = conf.getLedgerManagerType();
            if (lmType == null) {
                factoryClass = HierarchicalLedgerManagerFactory.class;
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

}
