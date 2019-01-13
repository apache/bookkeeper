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

import com.google.common.collect.Lists;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLEngine;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.common.conf.ConfigKey;
import org.apache.bookkeeper.common.conf.ConfigKeyGroup;
import org.apache.bookkeeper.common.conf.Type;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.common.util.JsonUtil.ParseJsonException;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.bookkeeper.util.StringEntryFormatter;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.lang.StringUtils;

/**
 * Abstract configuration.
 */
@Slf4j
public abstract class AbstractConfiguration<T extends AbstractConfiguration>
    extends CompositeConfiguration {

    public static final String READ_SYSTEM_PROPERTIES_PROPERTY = "org.apache.bookkeeper.conf.readsystemproperties";

    /**
     * Enable the use of System Properties, which was the default behaviour till 4.4.0.
     */
    private static final boolean READ_SYSTEM_PROPERTIES = Boolean.getBoolean(READ_SYSTEM_PROPERTIES_PROPERTY);

    protected static final ClassLoader DEFAULT_LOADER;
    static {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (null == loader) {
            loader = AbstractConfiguration.class.getClassLoader();
        }
        DEFAULT_LOADER = loader;
    }

    //
    // Metadata Service Settings
    //

    protected static final ConfigKeyGroup GROUP_METADATA_SERVICE = ConfigKeyGroup.builder("metadataservice")
        .description("Metadata Service related settings")
        .order(0)
        .build();

    protected static final String METADATA_SERVICE_URI = "metadataServiceUri";
    protected static final ConfigKey METADATA_SERVICE_URI_KEY = ConfigKey.builder(METADATA_SERVICE_URI)
        .type(Type.STRING)
        .description("metadata service uri that bookkeeper uses for loading corresponding metadata driver"
            + " and resolving its metadata service location")
        .required(true)
        .group(GROUP_METADATA_SERVICE)
        .orderInGroup(0)
        .build();

    protected static final String LEDGER_MANAGER_TYPE = "ledgerManagerType";
    protected static final String LEDGER_MANAGER_FACTORY_CLASS = "ledgerManagerFactoryClass";
    protected static final ConfigKey LEDGER_MANAGER_TYPE_KEY = ConfigKey.builder(LEDGER_MANAGER_TYPE)
        .type(Type.STRING)
        .description("Ledger Manager Type")
        .defaultValue("hierarchical")
        .group(GROUP_METADATA_SERVICE)
        .orderInGroup(1)
        .deprecated(true)
        .deprecatedByConfigKey(LEDGER_MANAGER_FACTORY_CLASS)
        .build();

    protected static final ConfigKey LEDGER_MANAGER_FACTORY_CLASS_KEY = ConfigKey.builder(LEDGER_MANAGER_FACTORY_CLASS)
        .type(Type.CLASS)
        .description("Ledger Manager Class")
        .documentation("What kind of ledger manager is used to manage how ledgers are stored, managed and"
            + " garbage collected. Try to read 'BookKeeper Internals' for detail info.")
        .defaultValue(HierarchicalLedgerManagerFactory.class)
        .group(GROUP_METADATA_SERVICE)
        .orderInGroup(2)
        .deprecated(true)
        .deprecatedByConfigKey(METADATA_SERVICE_URI)
        .deprecatedSince("4.7")
        .build();

    protected static final String ALLOW_SHADED_LEDGER_MANAGER_FACTORY_CLASS = "allowShadedLedgerManagerFactoryClass";
    protected static final ConfigKey ALLOW_SHADED_LEDGER_MANAGER_FACTORY_CLASS_KEY =
        ConfigKey.builder(ALLOW_SHADED_LEDGER_MANAGER_FACTORY_CLASS)
            .type(Type.BOOLEAN)
            .description("Flag to allow using shaded ledger manager class to connect to a bookkeeper cluster")
            .documentation("sometimes the bookkeeper server classes are shaded. the ledger manager factory"
                    + " classes might be relocated to be under other packages. this would fail the clients using"
                    + " shaded factory classes since the factory classes are not matched. Users can enable this flag"
                    + " to allow using shaded ledger manager class to connect to a bookkeeper cluster.")
            .defaultValue(false)
            .group(GROUP_METADATA_SERVICE)
            .orderInGroup(3)
            .build();


    protected static final String SHADED_LEDGER_MANAGER_FACTORY_CLASS_PREFIX = "shadedLedgerManagerFactoryClassPrefix";
    protected static final ConfigKey SHADED_LEDGER_MANAGER_FACTORY_CLASS_PREFIX_KEY =
        ConfigKey.builder(SHADED_LEDGER_MANAGER_FACTORY_CLASS_PREFIX)
            .type(Type.STRING)
            .description("the shaded ledger manager factory prefix")
            .documentation("this is used when `" + ALLOW_SHADED_LEDGER_MANAGER_FACTORY_CLASS + "` is set to true.")
            .defaultValue("dlshade.")
            .group(GROUP_METADATA_SERVICE)
            .orderInGroup(4)
            .build();

    // Kluge for compatibility testing. Never set this outside tests.
    public static final String LEDGER_MANAGER_FACTORY_DISABLE_CLASS_CHECK = "ledgerManagerFactoryDisableClassCheck";

    protected static final String ENABLE_BUSY_WAIT = "enableBusyWait";

    // Metastore settings, only being used when LEDGER_MANAGER_FACTORY_CLASS is MSLedgerManagerFactory
    protected static final String METASTORE_IMPL_CLASS = "metastoreImplClass";
    protected static final ConfigKey METASTORE_IMPL_CLASS_KEY = ConfigKey.builder(METASTORE_IMPL_CLASS)
        .type(Type.STRING)
        .description("metastore implementation class, only being used when `" + LEDGER_MANAGER_FACTORY_CLASS
            + "` is `MSLedgerManagerFactory`")
        .group(GROUP_METADATA_SERVICE)
        .orderInGroup(5)
        .build();

    protected static final String METASTORE_MAX_ENTRIES_PER_SCAN = "metastoreMaxEntriesPerScan";
    protected static final ConfigKey METASTORE_MAX_ENTRIES_PER_SCAN_KEY =
        ConfigKey.builder(METASTORE_MAX_ENTRIES_PER_SCAN)
            .type(Type.INT)
            .description("Max entries per scan in metastore, only being used when `" + LEDGER_MANAGER_FACTORY_CLASS
                + "` is `MSLedgerManagerFactory`")
            .defaultValue(50)
            .group(GROUP_METADATA_SERVICE)
            .orderInGroup(6)
            .build();

    //
    // ZooKeeper Metadata Service Settings
    //

    protected static final ConfigKeyGroup GROUP_ZK = ConfigKeyGroup.builder("zk")
        .description("ZooKeeper Metadata Service related settings")
        .order(1)
        .build();

    protected static final String AVAILABLE_NODE = "available";
    protected static final String ZK_LEDGERS_ROOT_PATH = "zkLedgersRootPath";
    protected static final ConfigKey ZK_LEDGERS_ROOT_PATH_KEY = ConfigKey.builder(ZK_LEDGERS_ROOT_PATH)
        .type(Type.STRING)
        .description("Root Zookeeper path to store ledger metadata")
        .documentation("This parameter is used by zookeeper-based ledger manager as a root znode"
            + " to store all ledgers.")
        .defaultValue("/ledgers")
        .group(GROUP_ZK)
        .orderInGroup(0)
        .deprecated(true)
        .deprecatedByConfigKey(METADATA_SERVICE_URI)
        .deprecatedSince("4.7")
        .build();
    protected static final String ZK_SERVERS = "zkServers";
    protected static final ConfigKey ZK_SERVERS_KEY = ConfigKey.builder(ZK_SERVERS)
        .type(Type.LIST)
        .description("A list of one of more servers on which Zookeeper is running")
        .documentation("The server list can be comma separated values, for example:"
            + " zkServers=zk1:2181,zk2:2181,zk3:2181")
        .required(true)
        .group(GROUP_ZK)
        .orderInGroup(1)
        .deprecated(true)
        .deprecatedByConfigKey(METADATA_SERVICE_URI)
        .deprecatedSince("4.7")
        .build();

    protected static final String ZK_TIMEOUT = "zkTimeout";
    protected static final ConfigKey ZK_TIMEOUT_KEY = ConfigKey.builder(ZK_TIMEOUT)
        .type(Type.INT)
        .description("ZooKeeper client session timeout in milliseconds")
        .documentation("Bookie server will exit if it received SESSION_EXPIRED because it"
            + " was partitioned off from ZooKeeper for more than the session timeout"
            + " JVM garbage collection, disk I/O will cause SESSION_EXPIRED."
            + " Increment this value could help avoiding this issue")
        .defaultValue(10000)
        .group(GROUP_ZK)
        .orderInGroup(2)
        .build();

    protected static final String ZK_REQUEST_RATE_LIMIT = "zkRequestRateLimit";
    protected static final ConfigKey ZK_REQUEST_RATE_LIMIT_KEY = ConfigKey.builder(ZK_REQUEST_RATE_LIMIT)
        .type(Type.DOUBLE)
        .description("The Zookeeper request limit")
        .documentation("It is only enabled when setting a positive value. Default value is 0.")
        .defaultValue(0.0f)
        .group(GROUP_ZK)
        .orderInGroup(3)
        .build();

    protected static final String ZK_ENABLE_SECURITY = "zkEnableSecurity";
    protected static final ConfigKey ZK_ENABLE_SECURITY_KEY = ConfigKey.builder(ZK_ENABLE_SECURITY)
        .type(Type.BOOLEAN)
        .description("Set ACLs on every node written on ZooKeeper")
        .documentation("this way only allowed users will be able to read and write BookKeeper"
            + " metadata stored on ZooKeeper. In order to make ACLs work you need to setup"
            + " ZooKeeper JAAS authentication all the bookies and Client need to share the"
            + " same user, and this is usually done using Kerberos authentication. See"
            + " ZooKeeper documentation")
        .defaultValue(false)
        .group(GROUP_ZK)
        .orderInGroup(4)
        .build();

    //
    // Security Settings
    //

    protected static final ConfigKeyGroup GROUP_SECURITY = ConfigKeyGroup.builder("security")
        .description("Security related settings")
        .order(2)
        .build();

    // Client auth provider factory class name. It must be configured on Bookies to for the Auditor
    protected static final String CLIENT_AUTH_PROVIDER_FACTORY_CLASS = "clientAuthProviderFactoryClass";
    protected static final ConfigKey CLIENT_AUTH_PROVIDER_FACTORY_CLASS_KEY =
        ConfigKey.builder(CLIENT_AUTH_PROVIDER_FACTORY_CLASS)
            .type(Type.CLASS)
            .description("Set the client authentication provider factory class name")
            .documentation("If Authentication is enabled on bookies and Auditor is running along"
                + " with bookies, this must be configured. Otherwise if this is not set, no authentication"
                + " will be used")
            .group(GROUP_SECURITY)
            .orderInGroup(1)
            .build();

    //
    // TLS Settings
    //

    protected static final ConfigKeyGroup GROUP_TLS = ConfigKeyGroup.builder("tls")
        .description("TLS Settings")
        .order(3)
        .build();

    // Common TLS configuration
    // TLS Provider (JDK or OpenSSL)
    protected static final String TLS_PROVIDER = "tlsProvider";
    protected static final ConfigKey TLS_PROVIDER_KEY = ConfigKey.builder(TLS_PROVIDER)
        .type(Type.STRING)
        .description("TLS Provider")
        .defaultValue("OpenSSL")
        .optionValues(Lists.newArrayList(
            "OpenSSL",
            "JDK"
        ))
        .group(GROUP_TLS)
        .orderInGroup(0)
        .build();

    // TLS provider factory class name
    protected static final String TLS_PROVIDER_FACTORY_CLASS = "tlsProviderFactoryClass";
    protected static final ConfigKey TLS_PROVIDER_FACTORY_CLASS_KEY = ConfigKey.builder(TLS_PROVIDER_FACTORY_CLASS)
        .type(Type.CLASS)
        .description("TLS Provider factory class name")
        .group(GROUP_TLS)
        .orderInGroup(1)
        .build();

    // Enable authentication of the other connection end point (mutual authentication)
    protected static final String TLS_CLIENT_AUTHENTICATION = "tlsClientAuthentication";
    protected static final ConfigKey TLS_CLIENT_AUTHENTICATION_KEY = ConfigKey.builder(TLS_CLIENT_AUTHENTICATION)
        .type(Type.BOOLEAN)
        .description("Enable authentication of the other connection endpoint (mutual authentication)")
        .defaultValue(false)
        .group(GROUP_TLS)
        .orderInGroup(2)
        .build();

    /**
     * TLS KeyStore, TrustStore, Password files and Certificate Paths.
     */
    protected static final String TLS_KEYSTORE_TYPE = "tlsKeyStoreType";
    protected static final ConfigKey TLS_KEYSTORE_TYPE_KEY = ConfigKey.builder(TLS_KEYSTORE_TYPE)
        .type(Type.STRING)
        .description("TLS Keystore type")
        .defaultValue("JKS")
        .optionValues(Lists.newArrayList(
            "PKCS12",
            "JKS",
            "PEM"
        ))
        .group(GROUP_TLS)
        .orderInGroup(3)
        .build();

    protected static final String TLS_KEYSTORE = "tlsKeyStore";
    protected static final ConfigKey TLS_KEYSTORE_KEY = ConfigKey.builder(TLS_KEYSTORE)
        .type(Type.STRING)
        .description("Path to TLS Keystore location")
        .group(GROUP_TLS)
        .orderInGroup(4)
        .build();

    protected static final String TLS_KEYSTORE_PASSWORD_PATH = "tlsKeyStorePasswordPath";
    protected static final ConfigKey TLS_KEYSTORE_PASSWORD_PATH_KEY = ConfigKey.builder(TLS_KEYSTORE_PASSWORD_PATH)
        .type(Type.STRING)
        .description("Path to TLS Keystore password location, if the key store is protected by a password")
        .group(GROUP_TLS)
        .orderInGroup(5)
        .build();

    protected static final String TLS_TRUSTSTORE_TYPE = "tlsTrustStoreType";
    protected static final ConfigKey TLS_TRUSTSTORE_TYPE_KEY = ConfigKey.builder(TLS_TRUSTSTORE_TYPE)
        .type(Type.STRING)
        .description("TLS Truststore type")
        .defaultValue("JKS")
        .optionValues(Lists.newArrayList(
            "PKCS12",
            "JKS",
            "PEM"
        ))
        .group(GROUP_TLS)
        .orderInGroup(6)
        .build();

    protected static final String TLS_TRUSTSTORE = "tlsTrustStore";
    protected static final ConfigKey TLS_TRUSTSTORE_KEY = ConfigKey.builder(TLS_TRUSTSTORE)
        .type(Type.STRING)
        .description("Path to TLS Truststore location")
        .group(GROUP_TLS)
        .orderInGroup(7)
        .build();

    protected static final String TLS_TRUSTSTORE_PASSWORD_PATH = "tlsTrustStorePasswordPath";
    protected static final ConfigKey TLS_TRUSTSTORE_PASSWORD_PATH_KEY = ConfigKey.builder(TLS_TRUSTSTORE_PASSWORD_PATH)
        .type(Type.STRING)
        .description("Path to TLS Truststore password location, if the trust store is protected by a password")
        .group(GROUP_TLS)
        .orderInGroup(8)
        .build();

    protected static final String TLS_CERTIFICATE_PATH = "tlsCertificatePath";
    protected static final ConfigKey TLS_CERTIFICATE_PATH_KEY = ConfigKey.builder(TLS_CERTIFICATE_PATH)
        .type(Type.STRING)
        .description("Path to TLS certificate location")
        .group(GROUP_TLS)
        .orderInGroup(9)
        .build();

    /**
     * This list will be passed to {@link SSLEngine#setEnabledCipherSuites(java.lang.String[]) }.
     * Please refer to official JDK JavaDocs
    */
    protected static final String TLS_ENABLED_CIPHER_SUITES = "tlsEnabledCipherSuites";
    protected static final ConfigKey TLS_ENABLED_CIPHER_SUITES_KEY = ConfigKey.builder(TLS_ENABLED_CIPHER_SUITES)
        .type(Type.STRING)
        .description("Set the list of enabled TLS cipher suites.")
        .documentation("Leave null not to override default JDK list. This list will be passed to"
            + " {@link SSLEngine#setEnabledCipherSuites(java.lang.String[]) }. Please refer to official JDK JavaDocs")
        .group(GROUP_TLS)
        .orderInGroup(10)
        .build();

    /**
     * This list will be passed to {@link SSLEngine#setEnabledProtocols(java.lang.String[]) }.
     * Please refer to official JDK JavaDocs
    */
    protected static final String TLS_ENABLED_PROTOCOLS = "tlsEnabledProtocols";
    protected static final ConfigKey TLS_ENABLED_PROTOCOLS_KEY = ConfigKey.builder(TLS_ENABLED_PROTOCOLS)
        .type(Type.STRING)
        .description("Set the list of enabled TLS protocols.")
        .documentation("Leave null not to override default JDK list. This list will be passed to"
            + " {@link SSLEngine#setEnabledProtocols(java.lang.String[]) }. Please refer to official JDK JavaDocs")
        .group(GROUP_TLS)
        .orderInGroup(11)
        .build();

    //
    // AutoRecovery
    //

    protected static final ConfigKeyGroup GROUP_AUTORECOVERY = ConfigKeyGroup.builder("autorecovery")
        .description("AutoRecovery related settings")
        .order(900)
        .build();

    protected static final ConfigKeyGroup GROUP_AUDITOR = ConfigKeyGroup.builder("auditor")
        .description("AutoRecovery Auditor related settings")
        .order(901)
        .build();

    protected static final String STORE_SYSTEMTIME_AS_LEDGER_UNDERREPLICATED_MARK_TIME =
        "storeSystemTimeAsLedgerUnderreplicatedMarkTime";
    protected static final ConfigKey STORE_SYSTEMTIME_AS_LEDGER_UNDERREPLICATED_MARK_TIME_KEY =
        ConfigKey.builder(STORE_SYSTEMTIME_AS_LEDGER_UNDERREPLICATED_MARK_TIME)
            .type(Type.BOOLEAN)
            .description("Enable the Auditor to use system time as underreplicated ledger mark time")
            .documentation("If this is enabled, Auditor will write a ctime field into the underreplicated"
                + " ledger znode")
            .defaultValue(true)
            .group(GROUP_AUDITOR)
            .orderInGroup(0)
            .build();

    protected static final ConfigKeyGroup GROUP_REPLICATION_WORKER = ConfigKeyGroup.builder("replicationworker")
        .description("AutoRecovery Replication Worker related settings")
        .order(902)
        .build();

    protected static final String REREPLICATION_ENTRY_BATCH_SIZE = "rereplicationEntryBatchSize";
    protected static final ConfigKey REREPLICATION_ENTRY_BATCH_SIZE_KEY =
        ConfigKey.builder(REREPLICATION_ENTRY_BATCH_SIZE)
            .type(Type.LONG)
            .description("The number of entries that a replication will rereplicate in parallel")
            .defaultValue(10)
            .group(GROUP_REPLICATION_WORKER)
            .orderInGroup(0)
            .build();

    //
    // Placement Policy
    //
    protected static final ConfigKeyGroup GROUP_PLACEMENT_POLICY = ConfigKeyGroup.builder("placementpolicy")
        .description("Placement policy related settings")
        .order(1000)
        .build();

    // minimum number of racks per write quorum
    public static final String MIN_NUM_RACKS_PER_WRITE_QUORUM = "minNumRacksPerWriteQuorum";
    protected static final ConfigKey MIN_NUM_RACKS_PER_WRITE_QUORUM_KEY =
        ConfigKey.builder(MIN_NUM_RACKS_PER_WRITE_QUORUM)
            .type(Type.INT)
            .description("minimum number of racks per write quorum")
            .documentation("RackawareEnsemblePlacementPolicy will try to get bookies from atleast '"
                + MIN_NUM_RACKS_PER_WRITE_QUORUM + "' racks for a writeQuorum")
            .defaultValue(2)
            .group(GROUP_PLACEMENT_POLICY)
            .orderInGroup(0)
            .build();

    // enforce minimum number of racks per write quorum
    public static final String ENFORCE_MIN_NUM_RACKS_PER_WRITE_QUORUM = "enforceMinNumRacksPerWriteQuorum";
    protected static final ConfigKey ENFORCE_MIN_NUM_RACKS_PER_WRITE_QUORUM_KEY =
        ConfigKey.builder(ENFORCE_MIN_NUM_RACKS_PER_WRITE_QUORUM)
            .type(Type.BOOLEAN)
            .description("Flag to enforce RackawareEnsemblePlacementPolicy to pick bookies from '"
                + MIN_NUM_RACKS_PER_WRITE_QUORUM + "' racks for a writeQuorum.")
            .documentation("If this feature is enabled, when a bookkeeper client cann't find an available bookie"
                + " then it would throw BKNotEnoughBookiesException instead of picking random one.")
            .defaultValue(false)
            .group(GROUP_PLACEMENT_POLICY)
            .orderInGroup(1)
            .build();

    //
    // Netty Settings
    //

    protected static final ConfigKeyGroup GROUP_NETTY = ConfigKeyGroup.builder("netty")
        .description("Netty related settings")
        .order(1001)
        .build();

    protected static final String NETTY_MAX_FRAME_SIZE = "nettyMaxFrameSizeBytes";
    protected static final int DEFAULT_NETTY_MAX_FRAME_SIZE = 5 * 1024 * 1024; // 5MB
    protected static final ConfigKey NETTY_MAX_FRAME_SIZE_KEY = ConfigKey.builder(NETTY_MAX_FRAME_SIZE)
        .type(Type.INT)
        .description("The maximum netty frame size in bytes")
        .documentation("Any message received larger than this will be rejected")
        .defaultValue(DEFAULT_NETTY_MAX_FRAME_SIZE)
        .group(GROUP_NETTY)
        .orderInGroup(0)
        .build();

    //
    // Tools related settings
    //

    protected static final ConfigKeyGroup GROUP_TOOLS = ConfigKeyGroup.builder("tools")
        .description("Tools Settings")
        .order(1002)
        .build();

    // Default formatter classes
    protected static final Class<? extends EntryFormatter> DEFAULT_ENTRY_FORMATTER = StringEntryFormatter.class;
    protected static final Class<? extends LedgerIdFormatter> DEFAULT_LEDGERID_FORMATTER =
            LedgerIdFormatter.LongLedgerIdFormatter.class;
    protected static final String LEDGERID_FORMATTER_CLASS = "ledgerIdFormatterClass";
    protected static final ConfigKey LEDGERID_FORMATTER_CLASS_KEY = ConfigKey.builder(LEDGERID_FORMATTER_CLASS)
        .type(Type.CLASS)
        .description("The formatter class that bookkeeper tools use to format ledger id")
        .defaultValue(DEFAULT_LEDGERID_FORMATTER)
        .group(GROUP_TOOLS)
        .orderInGroup(0)
        .build();

    protected static final String ENTRY_FORMATTER_CLASS = "entryFormatterClass";
    protected static final ConfigKey ENTRY_FORMATTER_CLASS_KEY = ConfigKey.builder(ENTRY_FORMATTER_CLASS)
        .type(Type.CLASS)
        .description("The formatter class that bookkeeper tools use to format entries")
        .defaultValue(DEFAULT_ENTRY_FORMATTER)
        .group(GROUP_TOOLS)
        .orderInGroup(1)
        .build();

    //
    // Monitoring related settings
    //

    protected static final ConfigKeyGroup GROUP_MONITORING = ConfigKeyGroup.builder("monitoring")
        .description("Monitoring related settings")
        .order(1003)
        .build();

    // Preserve MDC or not for tasks in executor
    protected static final String PRESERVE_MDC_FOR_TASK_EXECUTION = "preserveMdcForTaskExecution";
    protected static final ConfigKey PRESERVE_MDC_FOR_TASK_EXECUTION_KEY =
        ConfigKey.builder(PRESERVE_MDC_FOR_TASK_EXECUTION)
            .type(Type.BOOLEAN)
            .description("Flag to preserve MDC for tasks in Executor.")
            .defaultValue(false)
            .group(GROUP_MONITORING)
            .orderInGroup(0)
            .since("4.9")
            .build();

    // Allocator configuration
    protected static final String ALLOCATOR_POOLING_POLICY = "allocatorPoolingPolicy";
    protected static final String ALLOCATOR_POOLING_CONCURRENCY = "allocatorPoolingConcurrency";
    protected static final String ALLOCATOR_OOM_POLICY = "allocatorOutOfMemoryPolicy";
    protected static final String ALLOCATOR_LEAK_DETECTION_POLICY = "allocatorLeakDetectionPolicy";

    // option to limit stats logging
    public static final String LIMIT_STATS_LOGGING = "limitStatsLogging";
    protected static final ConfigKey LIMIT_STATS_LOGGING_KEY =
        ConfigKey.builder(LIMIT_STATS_LOGGING)
            .type(Type.BOOLEAN)
            .description("Flag to limit exposing stats (e.g. exposing pcbc stats)")
            .defaultValue(false)
            .group(GROUP_MONITORING)
            .orderInGroup(1)
            .since("4.9")
            .build();

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
    @SuppressWarnings("unchecked")
    public void loadConf(URL confURL) throws ConfigurationException {
        PropertiesConfiguration loadedConf = new PropertiesConfiguration(confURL);
        for (Iterator<String> iter = loadedConf.getKeys(); iter.hasNext(); ) {
            String key = iter.next();
            setProperty(key, loadedConf.getProperty(key));
        }
    }

    /**
     * You can load configuration from other configuration.
     *
     * @param baseConf
     *          Other Configuration
     */
    @SuppressWarnings("unchecked")
    public void loadConf(CompositeConfiguration baseConf) {
        for (Iterator<String> iter = baseConf.getKeys(); iter.hasNext(); ) {
            String key = iter.next();
            setProperty(key, baseConf.getProperty(key));
        }
    }

    /**
     * Get metadata service uri.
     *
     * <p><b>Warning:</b> this method silently converts checked exceptions to unchecked exceptions.
     * It is useful to use this method in lambda expressions. However it should not be used with places
     * which have logics to handle checked exceptions. In such cases use {@link #getMetadataServiceUri()} instead.
     *
     * @return metadata service uri
     * @throws UncheckedConfigurationException if the metadata service uri is invalid.
     */
    public String getMetadataServiceUriUnchecked() throws UncheckedConfigurationException {
        try {
            return getMetadataServiceUri();
        } catch (ConfigurationException e) {
            throw new UncheckedConfigurationException(e);
        }
    }

    /**
     * Get metadata service uri.
     *
     * @return metadata service uri.
     * @throws ConfigurationException if the metadata service uri is invalid.
     */
    public String getMetadataServiceUri() throws ConfigurationException {
        String serviceUri = METADATA_SERVICE_URI_KEY.getStringWithoutDefault(this);
        if (null == serviceUri) {
            // no service uri is defined, fallback to old settings
            String ledgerManagerType;
            ledgerManagerType = getLedgerManagerLayoutStringFromFactoryClass();
            String zkServers = getZkServers();
            if (null != zkServers) {
                // URI doesn't accept ','
                serviceUri = String.format(
                    "zk+%s://%s%s",
                    ledgerManagerType,
                    zkServers.replace(",", ";"),
                    getZkLedgersRootPath());
            }
        }
        return serviceUri;
    }

    /**
     * Set the metadata service uri.
     *
     * @param serviceUri the metadata service uri.
     * @return the configuration object.
     */
    public T setMetadataServiceUri(String serviceUri) {
        METADATA_SERVICE_URI_KEY.set(this, serviceUri);
        return getThis();
    }

    /**
     * Get zookeeper servers to connect.
     *
     * <p>`zkServers` is deprecating, in favor of using `metadataServiceUri`
     *
     * @return zookeeper servers
     * @deprecated since 4.7.0
     */
    @Deprecated
    public String getZkServers() {
        List servers = ZK_SERVERS_KEY.getList(this);
        if (null == servers || 0 == servers.size()) {
            return null;
        }
        return StringUtils.join(servers, ",");
    }

    /**
     * Set zookeeper servers to connect.
     *
     * <p>`zkServers` is deprecating, in favor of using `metadataServiceUri`
     *
     * @param zkServers
     *          ZooKeeper servers to connect
     */
    @Deprecated
    public T setZkServers(String zkServers) {
        ZK_SERVERS_KEY.set(this, Lists.newArrayList(StringUtils.split(zkServers, ",")));
        return getThis();
    }

    /**
     * Get zookeeper timeout.
     *
     * @return zookeeper server timeout
     */
    public int getZkTimeout() {
        return ZK_TIMEOUT_KEY.getInt(this);
    }

    /**
     * Set zookeeper timeout.
     *
     * @param zkTimeout
     *          ZooKeeper server timeout
     * @return server configuration
     */
    public T setZkTimeout(int zkTimeout) {
        ZK_TIMEOUT_KEY.set(this, zkTimeout);
        return getThis();
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
        LEDGER_MANAGER_TYPE_KEY.set(this, lmType);
    }

    /**
     * Get Ledger Manager Type.
     *
     * @return ledger manager type
     *
     * @deprecated replaced by {@link #getLedgerManagerFactoryClass()}
     */
    @Deprecated
    public String getLedgerManagerType() {
        return LEDGER_MANAGER_TYPE_KEY.getString(this);
    }

    /**
     * Set the flag to allow using shaded ledger manager factory class for
     * instantiating a ledger manager factory.
     *
     * @param allowed
     *          the flag to allow/disallow using shaded ledger manager factory class
     * @return configuration instance.
     */
    public T setAllowShadedLedgerManagerFactoryClass(boolean allowed) {
        ALLOW_SHADED_LEDGER_MANAGER_FACTORY_CLASS_KEY.set(this, allowed);
        return getThis();
    }

    /**
     * Is shaded ledger manager factory class name allowed to be used for
     * instantiating ledger manager factory.
     *
     * @return ledger manager factory class name.
     */
    public boolean isShadedLedgerManagerFactoryClassAllowed() {
        return ALLOW_SHADED_LEDGER_MANAGER_FACTORY_CLASS_KEY.getBoolean(this);
    }

    /**
     * Set the class prefix of the shaded ledger manager factory class for
     * instantiating a ledger manager factory.
     *
     * <p>This setting only takes effects when {@link #isShadedLedgerManagerFactoryClassAllowed()}
     * returns true.
     *
     * @param classPrefix
     *          the class prefix of shaded ledger manager factory class
     * @return configuration instance.
     */
    public T setShadedLedgerManagerFactoryClassPrefix(String classPrefix) {
        SHADED_LEDGER_MANAGER_FACTORY_CLASS_PREFIX_KEY.set(this, classPrefix);
        return getThis();
    }

    /**
     * Get the class prefix of the shaded ledger manager factory class name allowed to be used for
     * instantiating ledger manager factory.
     *
     * <p>This setting only takes effects when {@link #isShadedLedgerManagerFactoryClassAllowed()}
     * returns true
     *
     * @return ledger manager factory class name.
     * @see #isShadedLedgerManagerFactoryClassAllowed()
     */
    public String getShadedLedgerManagerFactoryClassPrefix() {
        return SHADED_LEDGER_MANAGER_FACTORY_CLASS_PREFIX_KEY.getString(this);
    }

    /**
     * Set Ledger Manager Factory Class Name.
     *
     * @param factoryClassName
     *          Ledger Manager Factory Class Name
     */
    public void setLedgerManagerFactoryClassName(String factoryClassName) {
        LEDGER_MANAGER_FACTORY_CLASS_KEY.set(this, factoryClassName);
    }

    /**
     * Get Ledger Manager Factory Class Name.
     *
     * @return ledger manager factory class name.
     */
    public String getLedgerManagerFactoryClassName() {
        return LEDGER_MANAGER_FACTORY_CLASS_KEY.getString(this);
    }

    /**
     * Get layout string ("null" if unconfigured).
     *
     * @return null, hierarchical, longhierarchical, or flat based on LEDGER_MANAGER_FACTORY_CLASS
     */
    @SuppressWarnings("deprecation")
    public String getLedgerManagerLayoutStringFromFactoryClass() throws ConfigurationException {
        String ledgerManagerType;
        Class<? extends LedgerManagerFactory> factoryClass = getLedgerManagerFactoryClass();
        if (factoryClass == null) {
            // set the ledger manager type to "null", so the driver implementation knows that the type is not set.
            ledgerManagerType = "null";
        } else {
            if (!AbstractZkLedgerManagerFactory.class.isAssignableFrom(factoryClass)) {
                // this is a non-zk implementation
                throw new ConfigurationException("metadata service uri is not supported for " + factoryClass);
            }
            if (factoryClass == HierarchicalLedgerManagerFactory.class) {
                ledgerManagerType = HierarchicalLedgerManagerFactory.NAME;
            } else if (factoryClass == org.apache.bookkeeper.meta.FlatLedgerManagerFactory.class) {
                ledgerManagerType = org.apache.bookkeeper.meta.FlatLedgerManagerFactory.NAME;
            } else if (factoryClass == LongHierarchicalLedgerManagerFactory.class) {
                ledgerManagerType = LongHierarchicalLedgerManagerFactory.NAME;
            } else if (factoryClass == org.apache.bookkeeper.meta.MSLedgerManagerFactory.class) {
                ledgerManagerType = org.apache.bookkeeper.meta.MSLedgerManagerFactory.NAME;
            } else {
                throw new IllegalArgumentException("Unknown zookeeper based ledger manager factory : "
                        + factoryClass);
            }
        }
        return ledgerManagerType;
    }

    /**
     * Set Ledger Manager Factory Class.
     *
     * @param factoryClass
     *          Ledger Manager Factory Class
     */
    public void setLedgerManagerFactoryClass(Class<? extends LedgerManagerFactory> factoryClass) {
        LEDGER_MANAGER_FACTORY_CLASS_KEY.set(this, factoryClass);
    }

    /**
     * Get ledger manager factory class.
     *
     * @return ledger manager factory class
     */
    public Class<? extends LedgerManagerFactory> getLedgerManagerFactoryClass()
        throws ConfigurationException {
        try {
            return LEDGER_MANAGER_FACTORY_CLASS_KEY.getClass(this, LedgerManagerFactory.class);
        } catch (IllegalArgumentException iae) {
            throw new ConfigurationException(iae.getMessage(), iae.getCause());
        }
    }

    /**
     * Set Zk Ledgers Root Path.
     *
     * @param zkLedgersPath zk ledgers root path
     */
    @Deprecated
    public void setZkLedgersRootPath(String zkLedgersPath) {
        ZK_LEDGERS_ROOT_PATH_KEY.set(this, zkLedgersPath);
    }

    /**
     * Get Zk Ledgers Root Path.
     *
     * @return zk ledgers root path
     */
    @Deprecated
    public String getZkLedgersRootPath() {
        return ZK_LEDGERS_ROOT_PATH_KEY.getString(this);
    }

    /**
     * Get zookeeper access request rate limit.
     *
     * @return zookeeper access request rate limit.
     */
    public double getZkRequestRateLimit() {
        return ZK_REQUEST_RATE_LIMIT_KEY.getDouble(this);
    }

    /**
     * Set zookeeper access request rate limit.
     *
     * @param rateLimit
     *          zookeeper access request rate limit.
     */
    public void setZkRequestRateLimit(double rateLimit) {
        ZK_REQUEST_RATE_LIMIT_KEY.set(this, rateLimit);
    }

    /**
     * Are z-node created with strict ACLs.
     *
     * @return usage of secure ZooKeeper ACLs
     */
    public boolean isZkEnableSecurity() {
        return ZK_ENABLE_SECURITY_KEY.getBoolean(this);
    }

    /**
     * Set the usage of ACLs of new z-nodes.
     *
     * @param zkEnableSecurity
     */
    public void setZkEnableSecurity(boolean zkEnableSecurity) {
        ZK_ENABLE_SECURITY_KEY.set(this, zkEnableSecurity);
    }

    /**
     * Get the node under which available bookies are stored.
     *
     * @return Node under which available bookies are stored.
     */
    @Deprecated
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
        REREPLICATION_ENTRY_BATCH_SIZE_KEY.set(this, rereplicationEntryBatchSize);
    }

    /**
     * Get the re-replication entry batch size.
     */
    public long getRereplicationEntryBatchSize() {
        return REREPLICATION_ENTRY_BATCH_SIZE_KEY.getLong(this);
    }

    /**
     * Get metastore implementation class.
     *
     * @return metastore implementation class name.
     */
    public String getMetastoreImplClass() {
        return METASTORE_IMPL_CLASS_KEY.getString(this);
    }

    /**
     * Set metastore implementation class.
     *
     * @param metastoreImplClass
     *          Metastore implementation Class name.
     */
    public void setMetastoreImplClass(String metastoreImplClass) {
        METASTORE_IMPL_CLASS_KEY.set(this, metastoreImplClass);
    }

    /**
     * Get max entries per scan in metastore.
     *
     * @return max entries per scan in metastore.
     */
    public int getMetastoreMaxEntriesPerScan() {
        return METASTORE_MAX_ENTRIES_PER_SCAN_KEY.getInt(this);
    }

    /**
     * Set max entries per scan in metastore.
     *
     * @param maxEntries
     *          Max entries per scan in metastore.
     */
    public void setMetastoreMaxEntriesPerScan(int maxEntries) {
        METASTORE_MAX_ENTRIES_PER_SCAN_KEY.set(this, maxEntries);
    }

    public void setFeature(String configProperty, Feature feature) {
        setProperty(configProperty, feature);
    }

    public Feature getFeature(String configProperty, Feature defaultValue) {
        if (null == getProperty(configProperty)) {
            return defaultValue;
        } else {
            return (Feature) getProperty(configProperty);
        }
    }

    /**
     * Set Ledger id formatter Class.
     *
     * @param formatterClass
     *          LedgerIdFormatter Class
     */
    public void setLedgerIdFormatterClass(Class<? extends LedgerIdFormatter> formatterClass) {
        LEDGERID_FORMATTER_CLASS_KEY.set(this, formatterClass);
    }

    /**
     * Get ledger id formatter class.
     *
     * @return LedgerIdFormatter class
     */
    public Class<? extends LedgerIdFormatter> getLedgerIdFormatterClass() {
        return LEDGERID_FORMATTER_CLASS_KEY.getClass(this, LedgerIdFormatter.class);
    }

    /**
     * Set entry formatter Class.
     *
     * @param formatterClass
     *          EntryFormatter Class
     */
    public void setEntryFormatterClass(Class<? extends EntryFormatter> formatterClass) {
        ENTRY_FORMATTER_CLASS_KEY.set(this, formatterClass);
    }

    /**
     * Get entry formatter class.
     *
     * @return EntryFormatter class
     */
    public Class<? extends EntryFormatter> getEntryFormatterClass(){
        return ENTRY_FORMATTER_CLASS_KEY.getClass(this, EntryFormatter.class);
    }

    /**
     * Set the client authentication provider factory class name.
     * If this is not set, no authentication will be used
     *
     * @param factoryClass
     *          the client authentication provider factory class name
     * @return client configuration
     */
    public T setClientAuthProviderFactoryClass(
            String factoryClass) {
        CLIENT_AUTH_PROVIDER_FACTORY_CLASS_KEY.set(this, factoryClass);
        return getThis();
    }

    /**
     * Get the client authentication provider factory class name.
     * If this returns null, no authentication will take place.
     *
     * @return the client authentication provider factory class name or null.
     */
    public String getClientAuthProviderFactoryClass() {
        return CLIENT_AUTH_PROVIDER_FACTORY_CLASS_KEY.getString(this);
    }

    /**
     * Get the maximum netty frame size in bytes.  Any message received larger
     * that this will be rejected.
     *
     * @return the maximum netty frame size in bytes.
     */
    public int getNettyMaxFrameSizeBytes() {
        return NETTY_MAX_FRAME_SIZE_KEY.getInt(this);
    }

    /**
     * Set the max number of bytes a single message can be that is read by the bookie.
     * Any message larger than that size will be rejected.
     *
     * @param maxSize
     *          the max size in bytes
     * @return server configuration
     */
    public T setNettyMaxFrameSizeBytes(int maxSize) {
        NETTY_MAX_FRAME_SIZE_KEY.set(this, maxSize);
        return getThis();
    }

    /**
     * Get the security provider factory class name. If this returns null, no security will be enforced on the channel.
     *
     * @return the security provider factory class name or null.
     */
    public String getTLSProviderFactoryClass() {
        return TLS_PROVIDER_FACTORY_CLASS_KEY.getString(this);
    }

    /**
     * Set the client security provider factory class name. If this is not set, no security will be used on the channel.
     *
     * @param factoryClass
     *            the client security provider factory class name
     * @return client configuration
     */
    public T setTLSProviderFactoryClass(String factoryClass) {
        TLS_PROVIDER_FACTORY_CLASS_KEY.set(this, factoryClass);
        return getThis();
    }

    /**
     * Get TLS Provider (JDK or OpenSSL).
     *
     * @return the TLS provider to use in creating TLS Context
     */
    public String getTLSProvider() {
        return TLS_PROVIDER_KEY.getString(this);
    }

    /**
     * Set TLS Provider (JDK or OpenSSL).
     *
     * @param provider
     *            TLS Provider type
     * @return Client Configuration
     */
    public T setTLSProvider(String provider) {
        TLS_PROVIDER_KEY.set(this, provider);
        return getThis();
    }

    /**
     * Whether the client will send an TLS certificate on TLS-handshake.
     *
     * @see #setTLSClientAuthentication(boolean)
     * @return whether TLS is enabled on the bookie or not.
     */
    public boolean getTLSClientAuthentication() {
        return TLS_CLIENT_AUTHENTICATION_KEY.getBoolean(this);
    }

    /**
     * Specify whether the client will send an TLS certificate on TLS-handshake.
     *
     * @param enabled
     *            Whether to send a certificate or not
     * @return client configuration
     */
    public T setTLSClientAuthentication(boolean enabled) {
        TLS_CLIENT_AUTHENTICATION_KEY.set(this, enabled);
        return getThis();
    }

    /**
     * Get the path to file containing TLS Certificate.
     *
     * @return
     */
    public String getTLSCertificatePath() {
        return TLS_CERTIFICATE_PATH_KEY.getString(this);
    }

    /**
     * Set the path to file containing TLS Certificate.
     *
     * @return
     */
    public T setTLSCertificatePath(String arg) {
        TLS_CLIENT_AUTHENTICATION_KEY.set(this, arg);
        return getThis();
    }

    /**
     * Set the list of enabled TLS cipher suites. Leave null not to override default JDK list. This list will be passed
     * to {@link SSLEngine#setEnabledCipherSuites(java.lang.String[]) }. Please refer to official JDK JavaDocs
     *
     * @param list
     *            comma separated list of enabled TLS cipher suites
     * @return current configuration
     */
    public T setTLSEnabledCipherSuites(
            String list) {
        TLS_ENABLED_CIPHER_SUITES_KEY.set(this, list);
        return getThis();
    }

    /**
     * Get the list of enabled TLS cipher suites.
     *
     * @return this list of enabled TLS cipher suites
     *
     * @see #setTLSEnabledCipherSuites(java.lang.String)
     */
    public String getTLSEnabledCipherSuites() {
        return TLS_ENABLED_CIPHER_SUITES_KEY.getString(this);
    }

    /**
     * Set the list of enabled TLS protocols. Leave null not to override default JDK list. This list will be passed to
     * {@link SSLEngine#setEnabledProtocols(java.lang.String[]) }. Please refer to official JDK JavaDocs
     *
     * @param list
     *            comma separated list of enabled TLS cipher suites
     * @return current configuration
     */
    public T setTLSEnabledProtocols(
            String list) {
        TLS_ENABLED_PROTOCOLS_KEY.set(this, list);
        return getThis();
    }

    /**
     * Get the list of enabled TLS protocols.
     *
     * @return the list of enabled TLS protocols.
     *
     * @see #setTLSEnabledProtocols(java.lang.String)
     */
    public String getTLSEnabledProtocols() {
        return TLS_ENABLED_PROTOCOLS_KEY.getString(this);
    }

    /**
     * Set the minimum number of racks per write quorum.
     */
    public void setMinNumRacksPerWriteQuorum(int minNumRacksPerWriteQuorum) {
        MIN_NUM_RACKS_PER_WRITE_QUORUM_KEY.set(this, minNumRacksPerWriteQuorum);
    }

    /**
     * Get the minimum number of racks per write quorum.
     */
    public int getMinNumRacksPerWriteQuorum() {
        return MIN_NUM_RACKS_PER_WRITE_QUORUM_KEY.getInt(this);
    }

    /**
     * Set the flag to enforce minimum number of racks per write quorum.
     */
    public void setEnforceMinNumRacksPerWriteQuorum(boolean enforceMinNumRacksPerWriteQuorum) {
        ENFORCE_MIN_NUM_RACKS_PER_WRITE_QUORUM_KEY.set(this, enforceMinNumRacksPerWriteQuorum);
    }

    /**
     * Get the flag which enforces the minimum number of racks per write quorum.
     */
    public boolean getEnforceMinNumRacksPerWriteQuorum() {
        return ENFORCE_MIN_NUM_RACKS_PER_WRITE_QUORUM_KEY.getBoolean(this);
    }

    /**
     * Enable the Auditor to use system time as underreplicated ledger mark
     * time.
     *
     * <p>If this is enabled, Auditor will write a ctime field into the
     * underreplicated ledger znode.
     *
     * @param enabled
     *            flag to enable/disable Auditor using system time as
     *            underreplicated ledger mark time.
     */
    public T setStoreSystemTimeAsLedgerUnderreplicatedMarkTime(boolean enabled) {
        STORE_SYSTEMTIME_AS_LEDGER_UNDERREPLICATED_MARK_TIME_KEY.set(this, enabled);
        return getThis();
    }

    /**
     * Return the flag that indicates whether auditor is using system time as
     * underreplicated ledger mark time.
     *
     * @return the flag that indicates whether auditor is using system time as
     *         underreplicated ledger mark time.
     */
    public boolean getStoreSystemTimeAsLedgerUnderreplicatedMarkTime() {
        return STORE_SYSTEMTIME_AS_LEDGER_UNDERREPLICATED_MARK_TIME_KEY.getBoolean(this);
    }

    /**
     * Whether to preserve MDC for tasks in Executor.
     *
     * @return flag to enable/disable MDC preservation in Executor.
     */
    public boolean getPreserveMdcForTaskExecution() {
        return PRESERVE_MDC_FOR_TASK_EXECUTION_KEY.getBoolean(this);
    }

    /**
     * Whether to preserve MDC for tasks in Executor.
     *
     * @param enabled
     *          flag to enable/disable MDC preservation in Executor.
     * @return configuration.
     */
    public T setPreserveMdcForTaskExecution(boolean enabled) {
        PRESERVE_MDC_FOR_TASK_EXECUTION_KEY.set(this, enabled);
        return getThis();
    }

    /**
     * @return the configured pooling policy for the allocator.
     */
    public PoolingPolicy getAllocatorPoolingPolicy() {
        return PoolingPolicy.valueOf(this.getString(ALLOCATOR_POOLING_POLICY, PoolingPolicy.PooledDirect.toString()));
    }

    /**
     * Define the memory pooling policy.
     *
     * <p>Default is {@link PoolingPolicy#PooledDirect}
     *
     * @param poolingPolicy
     *            the memory pooling policy
     * @return configuration object.
     */
    public T setAllocatorPoolingPolicy(PoolingPolicy poolingPolicy) {
        this.setProperty(ALLOCATOR_POOLING_POLICY, poolingPolicy.toString());
        return getThis();
    }

    /**
     * @return the configured pooling concurrency for the allocator.
     */
    public int getAllocatorPoolingConcurrency() {
        return this.getInteger(ALLOCATOR_POOLING_CONCURRENCY, 2 * Runtime.getRuntime().availableProcessors());
    }

    /**
     * Controls the amount of concurrency for the memory pool.
     *
     * <p>Default is to have a number of allocator arenas equals to 2 * CPUS.
     *
     * <p>Decreasing this number will reduce the amount of memory overhead, at the
     * expense of increased allocation contention.
     *
     * @param concurrency
     *            the concurrency level to use for the allocator pool
     * @return configuration object.
     */
    public T setAllocatorPoolingConcurrenncy(int concurrency) {
        this.setProperty(ALLOCATOR_POOLING_POLICY, concurrency);
        return getThis();
    }

    /**
     * @return the configured ouf of memory policy for the allocator.
     */
    public OutOfMemoryPolicy getAllocatorOutOfMemoryPolicy() {
        return OutOfMemoryPolicy
                .valueOf(this.getString(ALLOCATOR_OOM_POLICY, OutOfMemoryPolicy.FallbackToHeap.toString()));
    }

    /**
     * Define the memory allocator out of memory policy.
     *
     * <p>Default is {@link OutOfMemoryPolicy#FallbackToHeap}
     *
     * @param oomPolicy
     *            the "out-of-memory" policy for the memory allocator
     * @return configuration object.
     */
    public T setAllocatorOutOfMemoryPolicy(OutOfMemoryPolicy oomPolicy) {
        this.setProperty(ALLOCATOR_OOM_POLICY, oomPolicy.toString());
        return getThis();
    }

    /**
     * Return the configured leak detection policy for the allocator.
     */
    public LeakDetectionPolicy getAllocatorLeakDetectionPolicy() {
        return LeakDetectionPolicy
                .valueOf(this.getString(ALLOCATOR_LEAK_DETECTION_POLICY, LeakDetectionPolicy.Disabled.toString()));
    }

    /**
     * Enable the leak detection for the allocator.
     *
     * <p>Default is {@link LeakDetectionPolicy#Disabled}
     *
     * @param leakDetectionPolicy
     *            the leak detection policy for the memory allocator
     * @return configuration object.
     */
    public T setAllocatorLeakDetectionPolicy(LeakDetectionPolicy leakDetectionPolicy) {
        this.setProperty(ALLOCATOR_LEAK_DETECTION_POLICY, leakDetectionPolicy.toString());
        return getThis();
    }

    /**
     * Return whether the busy-wait is enabled for BookKeeper and Netty IO threads.
     *
     * <p>Default is false
     *
     * @return the value of the option
     */
    public boolean isBusyWaitEnabled() {
        return getBoolean(ENABLE_BUSY_WAIT, false);
    }

    /**
     * Option to enable busy-wait settings.
     *
     * <p>Default is false.
     *
     * <p>WARNING: This option will enable spin-waiting on executors and IO threads
     * in order to reduce latency during context switches. The spinning will
     * consume 100% CPU even when bookie is not doing any work. It is
     * recommended to reduce the number of threads in the main workers pool
     * ({@link ClientConfiguration#setNumWorkerThreads(int)}) and Netty event
     * loop {@link ClientConfiguration#setNumIOThreads(int)} to only have few
     * CPU cores busy.
     * </p>
     *
     * @param busyWaitEanbled
     *            if enabled, use spin-waiting strategy to reduce latency in
     *            context switches
     *
     * @see #isBusyWaitEnabled()
     */
    public T setBusyWaitEnabled(boolean busyWaitEanbled) {
        setProperty(ENABLE_BUSY_WAIT, busyWaitEanbled);
        return getThis();
    }

    /**
     * Return the flag indicating whether to limit stats logging.
     *
     * @return
     *      the boolean flag indicating whether to limit stats logging
     */
    public boolean getLimitStatsLogging() {
        return LIMIT_STATS_LOGGING_KEY.getBoolean(this);
    }

    /**
     * Sets flag to limit the stats logging.
     *
     * @param limitStatsLogging
     *          flag to limit the stats logging.
     * @return configuration.
     */
    public T setLimitStatsLogging(boolean limitStatsLogging) {
        LIMIT_STATS_LOGGING_KEY.set(this, limitStatsLogging);
        return getThis();
    }

    /**
     * Trickery to allow inheritance with fluent style.
     */
    protected abstract T getThis();

    /**
     * returns the string representation of json format of this config.
     *
     * @return
     * @throws ParseJsonException
     */
    public String asJson() throws ParseJsonException {
        return JsonUtil.toJson(toMap());
    }

    private Map<String, Object> toMap() {
        Map<String, Object> configMap = new HashMap<>();
        Iterator<String> iterator = this.getKeys();
        while (iterator.hasNext()) {
            String key = iterator.next().toString();
            Object property = this.getProperty(key);
            if (property != null) {
                configMap.put(key, property.toString());
            }
        }
        return configMap;
    }
}
