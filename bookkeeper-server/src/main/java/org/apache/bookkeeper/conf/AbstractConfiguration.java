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
import javax.net.ssl.SSLEngine;
import static org.apache.bookkeeper.conf.ClientConfiguration.CLIENT_AUTH_PROVIDER_FACTORY_CLASS;

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

    // Common SSL configuration

    /**
     * This list will be passed to {@link SSLEngine#setEnabledCipherSuites(java.lang.String[]) }.
     * Please refer to official JDK JavaDocs
    */
    protected final static String SSL_ENABLED_CIPHER_SUITES = "sslEnabledCipherSuites";

    /**
     * This list will be passed to {@link SSLEngine#setEnabledProtocols(java.lang.String[]) }.
     * Please refer to official JDK JavaDocs
    */
    protected final static String SSL_ENABLED_PROTOCOLS = "sslEnabledProtocols";

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
     * Set the list of enabled SSL cipher suites. Leave null not to override default JDK list.
     * This list will be passed to {@link SSLEngine#setEnabledCipherSuites(java.lang.String[]) }.
     * Please refer to official JDK JavaDocs
     *
     * @param list comma separated list of enabled SSL cipher suites
     * @return current configuration
     */
    public AbstractConfiguration setSslEnabledCipherSuites(
            String list) {
        setProperty(SSL_ENABLED_CIPHER_SUITES, list);
        return this;
    }

    /**
     * Get the list of enabled SSL cipher suites
     *
     * @return this list of enabled SSL cipher suites
     *
     * @see #setSslEnabledCipherSuites(java.lang.String)
     */
    public String getSslEnabledCipherSuites() {
        return getString(SSL_ENABLED_CIPHER_SUITES, null);
    }

    /**
     * Set the list of enabled SSL protocols. Leave null not to override default JDK list.
     * This list will be passed to {@link SSLEngine#setEnabledProtocols(java.lang.String[]) }.
     * Please refer to official JDK JavaDocs
     *
     * @param list comma separated list of enabled SSL cipher suites
     * @return current configuration
     */
    public AbstractConfiguration setSslEnabledProtocols(
            String list) {
        setProperty(SSL_ENABLED_PROTOCOLS, list);
        return this;
    }

    /**
     * Get the list of enabled SSL protocols
     *
     * @return the list of enabled SSL protocols.
     *
     * @see #setSslEnabledProtocols(java.lang.String)
     */
    public String getSslEnabledProtocols() {
        return getString(SSL_ENABLED_PROTOCOLS, null);
    }
}
