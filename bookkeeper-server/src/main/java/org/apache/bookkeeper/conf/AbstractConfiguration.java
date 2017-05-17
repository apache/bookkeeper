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
package org.apache.bookkeeper.conf;

import java.net.URL;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;

import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract configuration
 */
public abstract class AbstractConfiguration extends CompositeConfiguration {

    static final Logger LOG = LoggerFactory.getLogger(AbstractConfiguration.class);
    public static final String READ_SYSTEM_PROPERTIES_PROPERTY
                            = "org.apache.bookkeeper.conf.readsystemproperties";
    /**
     * Enable the use of System Properties, which was the default behaviour till 4.4.0
     */
    private static final boolean READ_SYSTEM_PROPERTIES
                                    = Boolean.getBoolean(READ_SYSTEM_PROPERTIES_PROPERTY);

    protected static final ClassLoader defaultLoader;
    static {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (null == loader) {
            loader = AbstractConfiguration.class.getClassLoader();
        }
        defaultLoader = loader;
    }

    // Ledger Manager
    protected final static String LEDGER_MANAGER_TYPE = "ledgerManagerType";
    protected final static String LEDGER_MANAGER_FACTORY_CLASS = "ledgerManagerFactoryClass";
    protected final static String ZK_LEDGERS_ROOT_PATH = "zkLedgersRootPath";
    protected final static String AVAILABLE_NODE = "available";
    protected final static String REREPLICATION_ENTRY_BATCH_SIZE = "rereplicationEntryBatchSize";

    // Metastore settings, only being used when LEDGER_MANAGER_FACTORY_CLASS is MSLedgerManagerFactory
    protected final static String METASTORE_IMPL_CLASS = "metastoreImplClass";
    protected final static String METASTORE_MAX_ENTRIES_PER_SCAN = "metastoreMaxEntriesPerScan";

    // Client auth provider factory class name. It must be configured on Bookies to for the Auditor
    protected final static String CLIENT_AUTH_PROVIDER_FACTORY_CLASS = "clientAuthProviderFactoryClass";

    //Netty configuration
    protected final static String NETTY_MAX_FRAME_SIZE = "nettyMaxFrameSizeBytes";
    protected final static int DEFAULT_NETTY_MAX_FRAME_SIZE = 5 * 1024 * 1024; // 5MB

    // Zookeeper ACL settings
    protected final static String ZK_ENABLE_SECURITY = "zkEnableSecurity";

    // Kluge for compatibility testing. Never set this outside tests.
    public final static String LEDGER_MANAGER_FACTORY_DISABLE_CLASS_CHECK = "ledgerManagerFactoryDisableClassCheck";

    protected AbstractConfiguration() {
        super();
        if (READ_SYSTEM_PROPERTIES) {
            // add configuration for system properties
            addConfiguration(new SystemConfiguration());
        }
    }

    /**
     * You can load configurations in precedence order. The first one takes
     * precedence over any loaded later.
     *
     * @param confURL
     *          Configuration URL
     */
    public void loadConf(URL confURL) throws ConfigurationException {
        Configuration loadedConf = new PropertiesConfiguration(confURL);
        addConfiguration(loadedConf);
    }

    /**
     * You can load configuration from other configuration
     *
     * @param baseConf
     *          Other Configuration
     */
    public void loadConf(AbstractConfiguration baseConf) {
        addConfiguration(baseConf); 
    }

    /**
     * Load configuration from other configuration object
     *
     * @param otherConf
     *          Other configuration object
     */
    public void loadConf(Configuration otherConf) {
        addConfiguration(otherConf);
    }

    /**
     * Set Ledger Manager Type.
     *
     * @param lmType
     *          Ledger Manager Type
     * @deprecated replaced by {@link #setLedgerManagerFactoryClass}
     */
    @Deprecated
    public void setLedgerManagerType(String lmType) {
        setProperty(LEDGER_MANAGER_TYPE, lmType); 
    }

    /**
     * Get Ledger Manager Type.
     *
     * @return ledger manager type
     * @throws ConfigurationException
     * @deprecated replaced by {@link #getLedgerManagerFactoryClass()}
     */
    @Deprecated
    public String getLedgerManagerType() {
        return getString(LEDGER_MANAGER_TYPE);
    }

    /**
     * Set Ledger Manager Factory Class Name.
     *
     * @param factoryClassName
     *          Ledger Manager Factory Class Name
     */
    public void setLedgerManagerFactoryClassName(String factoryClassName) {
        setProperty(LEDGER_MANAGER_FACTORY_CLASS, factoryClassName);
    }

    /**
     * Set Ledger Manager Factory Class.
     *
     * @param factoryClass
     *          Ledger Manager Factory Class
     */
    public void setLedgerManagerFactoryClass(Class<? extends LedgerManagerFactory> factoryClass) {
        setProperty(LEDGER_MANAGER_FACTORY_CLASS, factoryClass.getName());
    }

    /**
     * Get ledger manager factory class.
     *
     * @return ledger manager factory class
     */
    public Class<? extends LedgerManagerFactory> getLedgerManagerFactoryClass()
        throws ConfigurationException {
        return ReflectionUtils.getClass(this, LEDGER_MANAGER_FACTORY_CLASS,
                                        null, LedgerManagerFactory.class,
                                        defaultLoader);
    }

    /**
     * Set Zk Ledgers Root Path.
     *
     * @param zkLedgersPath zk ledgers root path
     */
    public void setZkLedgersRootPath(String zkLedgersPath) {
        setProperty(ZK_LEDGERS_ROOT_PATH, zkLedgersPath);
    }

    /**
     * Get Zk Ledgers Root Path.
     *
     * @return zk ledgers root path
     */
    public String getZkLedgersRootPath() {
        return getString(ZK_LEDGERS_ROOT_PATH, "/ledgers");
    }

    /**
     * Are z-node created with strict ACLs
     *
     * @return usage of secure ZooKeeper ACLs
     */
    public boolean isZkEnableSecurity() {
        return getBoolean(ZK_ENABLE_SECURITY, false);
    }

    /**
     * Set the usage of ACLs of new z-nodes
     *
     * @param zkEnableSecurity
     */
    public void setZkEnableSecurity(boolean zkEnableSecurity) {
        setProperty(ZK_ENABLE_SECURITY, zkEnableSecurity);
    }

    /**
     * Get the node under which available bookies are stored
     *
     * @return Node under which available bookies are stored.
     */
    public String getZkAvailableBookiesPath() {
        return getZkLedgersRootPath() + "/" + AVAILABLE_NODE;
    }

    /**
     * Set the max entries to keep in fragment for re-replication. If fragment
     * has more entries than this count, then the original fragment will be
     * split into multiple small logical fragments by keeping max entries count
     * to rereplicationEntryBatchSize. So, re-replication will happen in batches
     * wise.
     */
    public void setRereplicationEntryBatchSize(long rereplicationEntryBatchSize) {
        setProperty(REREPLICATION_ENTRY_BATCH_SIZE, rereplicationEntryBatchSize);
    }

    /**
     * Get the re-replication entry batch size
     */
    public long getRereplicationEntryBatchSize() {
        return getLong(REREPLICATION_ENTRY_BATCH_SIZE, 10);
    }

    /**
     * Get metastore implementation class.
     *
     * @return metastore implementation class name.
     */
    public String getMetastoreImplClass() {
        return getString(METASTORE_IMPL_CLASS);
    }

    /**
     * Set metastore implementation class.
     *
     * @param metastoreImplClass
     *          Metastore implementation Class name.
     */
    public void setMetastoreImplClass(String metastoreImplClass) {
        setProperty(METASTORE_IMPL_CLASS, metastoreImplClass);
    }

    /**
     * Get max entries per scan in metastore.
     *
     * @return max entries per scan in metastore.
     */
    public int getMetastoreMaxEntriesPerScan() {
        return getInt(METASTORE_MAX_ENTRIES_PER_SCAN, 50);
    }

    /**
     * Set max entries per scan in metastore.
     *
     * @param maxEntries
     *          Max entries per scan in metastore.
     */
    public void setMetastoreMaxEntriesPerScan(int maxEntries) {
        setProperty(METASTORE_MAX_ENTRIES_PER_SCAN, maxEntries);
    }

    public void setFeature(String configProperty, Feature feature) {
        setProperty(configProperty, feature);
    }

    public Feature getFeature(String configProperty, Feature defaultValue) {
        if (null == getProperty(configProperty)) {
            return defaultValue;
        } else {
            return (Feature)getProperty(configProperty);
        }
    }

    /**
     * Set the client authentication provider factory class name.
     * If this is not set, no authentication will be used
     *
     * @param factoryClass
     *          the client authentication provider factory class name
     * @return client configuration
     */
    public AbstractConfiguration setClientAuthProviderFactoryClass(
            String factoryClass) {
        setProperty(CLIENT_AUTH_PROVIDER_FACTORY_CLASS, factoryClass);
        return this;
    }

    /**
     * Get the client authentication provider factory class name. If this returns null, no authentication will take
     * place.
     *
     * @return the client authentication provider factory class name or null.
     */
    public String getClientAuthProviderFactoryClass() {
        return getString(CLIENT_AUTH_PROVIDER_FACTORY_CLASS, null);
    }

    /**
     * Get the maximum netty frame size in bytes.  Any message received larger
     * that this will be rejected.
     *
     * @return the maximum netty frame size in bytes.
     */
    public int getNettyMaxFrameSizeBytes() {
        return getInt(NETTY_MAX_FRAME_SIZE, DEFAULT_NETTY_MAX_FRAME_SIZE);
    }

    /**
     * Set the max number of bytes a single message can be that is read by the bookie.
     * Any message larger than that size will be rejected.
     *
     * @param maxSize
     *          the max size in bytes
     * @return server configuration
     */
    public AbstractConfiguration setNettyMaxFrameSizeBytes(int maxSize) {
        setProperty(NETTY_MAX_FRAME_SIZE, String.valueOf(maxSize));
        return this;
    }
}
