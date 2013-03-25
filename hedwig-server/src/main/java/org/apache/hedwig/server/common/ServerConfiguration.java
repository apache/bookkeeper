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
package org.apache.hedwig.server.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.hedwig.conf.AbstractConfiguration;
import org.apache.hedwig.server.meta.MetadataManagerFactory;
import org.apache.hedwig.util.HedwigSocketAddress;

public class ServerConfiguration extends AbstractConfiguration {
    public final static String REGION = "region";
    protected final static String MAX_MESSAGE_SIZE = "max_message_size";
    protected final static String READAHEAD_COUNT = "readahead_count";
    protected final static String READAHEAD_SIZE = "readahead_size";
    protected final static String CACHE_SIZE = "cache_size";
    protected final static String CACHE_ENTRY_TTL = "cache_entry_ttl";
    protected final static String SCAN_BACKOFF_MSEC = "scan_backoff_ms";
    protected final static String SERVER_PORT = "server_port";
    protected final static String SSL_SERVER_PORT = "ssl_server_port";
    protected final static String ZK_PREFIX = "zk_prefix";
    protected final static String ZK_HOST = "zk_host";
    protected final static String ZK_TIMEOUT = "zk_timeout";
    protected final static String READAHEAD_ENABLED = "readahead_enabled";
    protected final static String STANDALONE = "standalone";
    protected final static String REGIONS = "regions";
    protected final static String CERT_NAME = "cert_name";
    protected final static String CERT_PATH = "cert_path";
    protected final static String PASSWORD = "password";
    protected final static String SSL_ENABLED = "ssl_enabled";
    protected final static String CONSUME_INTERVAL = "consume_interval";
    protected final static String INIT_NUM_TOPICS = "init_num_topics";
    protected final static String MAX_NUM_TOPICS = "max_num_topics";
    protected final static String RETENTION_SECS = "retention_secs";
    protected final static String RETENTION_SECS_AFTER_ACCESS = "retention_secs_after_access";
    protected final static String INTER_REGION_SSL_ENABLED = "inter_region_ssl_enabled";
    protected final static String MESSAGES_CONSUMED_THREAD_RUN_INTERVAL = "messages_consumed_thread_run_interval";
    protected final static String BK_ENSEMBLE_SIZE = "bk_ensemble_size";
    @Deprecated
    protected final static String BK_QUORUM_SIZE = "bk_quorum_size";
    protected final static String BK_WRITE_QUORUM_SIZE = "bk_write_quorum_size";
    protected final static String BK_ACK_QUORUM_SIZE = "bk_ack_quorum_size";
    protected final static String RETRY_REMOTE_SUBSCRIBE_THREAD_RUN_INTERVAL = "retry_remote_subscribe_thread_run_interval";
    protected final static String DEFAULT_MESSAGE_WINDOW_SIZE =
        "default_message_window_size";
    protected final static String NUM_READAHEAD_CACHE_THREADS = "num_readahead_cache_threads";
    protected final static String NUM_DELIVERY_THREADS = "num_delivery_threads";

    protected final static String MAX_ENTRIES_PER_LEDGER = "max_entries_per_ledger";

    // manager related settings
    protected final static String METADATA_MANAGER_BASED_TOPIC_MANAGER_ENABLED = "metadata_manager_based_topic_manager_enabled";
    protected final static String METADATA_MANAGER_FACTORY_CLASS = "metadata_manager_factory_class";

    // metastore settings, only being used when METADATA_MANAGER_FACTORY_CLASS is MsMetadataManagerFactory
    protected final static String METASTORE_IMPL_CLASS = "metastore_impl_class";
    protected final static String METASTORE_MAX_ENTRIES_PER_SCAN = "metastoreMaxEntriesPerScan";

    private static ClassLoader defaultLoader;
    static {
        defaultLoader = Thread.currentThread().getContextClassLoader();
        if (null == defaultLoader) {
            defaultLoader = ServerConfiguration.class.getClassLoader();
        }
    }

    // these are the derived attributes
    protected ByteString myRegionByteString = null;
    protected HedwigSocketAddress myServerAddress = null;
    protected List<String> regionList = null;

    // Although this method is not currently used, currently maintaining it like
    // this so that we can support on-the-fly changes in configuration
    protected void refreshDerivedAttributes() {
        refreshMyRegionByteString();
        refreshMyServerAddress();
        refreshRegionList();
    }

    @Override
    public void loadConf(URL confURL) throws ConfigurationException {
        super.loadConf(confURL);
        refreshDerivedAttributes();
    }

    public int getMaximumMessageSize() {
        return conf.getInt(MAX_MESSAGE_SIZE, 1258291); /* 1.2M */
    }

    public String getMyRegion() {
        return conf.getString(REGION, "standalone");
    }

    protected void refreshMyRegionByteString() {
        myRegionByteString = ByteString.copyFromUtf8(getMyRegion());
    }

    protected void refreshMyServerAddress() {
        try {
            // Use the raw IP address as the hostname
            myServerAddress = new HedwigSocketAddress(InetAddress.getLocalHost().getHostAddress(), getServerPort(),
                    getSSLServerPort());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    // The expected format for the regions parameter is Hostname:Port:SSLPort
    // with spaces in between each of the regions.
    protected void refreshRegionList() {
        String regions = conf.getString(REGIONS, "");
        if (regions.isEmpty()) {
            regionList = new LinkedList<String>();
        } else {
            regionList = Arrays.asList(regions.split(" "));
        }
    }

    public ByteString getMyRegionByteString() {
        if (myRegionByteString == null) {
            refreshMyRegionByteString();
        }
        return myRegionByteString;
    }

    /**
     * Maximum number of messages to read ahead. Default is 10.
     * 
     * @return int
     */
    public int getReadAheadCount() {
        return conf.getInt(READAHEAD_COUNT, 10);
    }

    /**
     * Maximum number of bytes to read ahead. Default is 4MB.
     * 
     * @return long
     */
    public long getReadAheadSizeBytes() {
        return conf.getLong(READAHEAD_SIZE, 4 * 1024 * 1024); // 4M
    }

    /**
     * Maximum cache size. By default is the smallest of 2G or
     * half the heap size.
     * 
     * @return long
     */
    public long getMaximumCacheSize() {
        // 2G or half of the maximum amount of memory the JVM uses
        return conf.getLong(CACHE_SIZE, Math.min(2 * 1024L * 1024L * 1024L, Runtime.getRuntime().maxMemory() / 2));
    }

    /**
     * Cache Entry TTL. By default is 0, cache entry will not be evicted
     * until the cache is fullfilled or the messages are already consumed.
     * The TTL is only checked when trying adding a new entry into the cache.
     *
     * @return cache entry ttl.
     */
    public long getCacheEntryTTL() {
        return conf.getLong(CACHE_ENTRY_TTL, 0L);
    }

    /**
     * After a scan of a log fails, how long before we retry (in msec)
     * 
     * @return long
     */
    public long getScanBackoffPeriodMs() {
        return conf.getLong(SCAN_BACKOFF_MSEC, 1000);
    }
    
    /**
     * Returns server port.
     * 
     * @return int
     */
    public int getServerPort() {
        return conf.getInt(SERVER_PORT, 4080);
    }

    /**
     * Returns SSL server port.
     * 
     * @return int
     */
    public int getSSLServerPort() {
        return conf.getInt(SSL_SERVER_PORT, 9876);
    }

    /**
     * Returns ZooKeeper path prefix.
     * 
     * @return string
     */
    public String getZkPrefix() {
        return conf.getString(ZK_PREFIX, "/hedwig");
    }

    public StringBuilder getZkRegionPrefix(StringBuilder sb) {
        return sb.append(getZkPrefix()).append("/").append(getMyRegion());
    }

    /**
     * Get znode path to store manager layouts.
     *
     * @param sb
     *          StringBuilder to store znode path to store manager layouts.
     * @return znode path to store manager layouts.
     */
    public StringBuilder getZkManagersPrefix(StringBuilder sb) {
        return getZkRegionPrefix(sb).append("/managers");
    }

    public StringBuilder getZkTopicsPrefix(StringBuilder sb) {
        return getZkRegionPrefix(sb).append("/topics");
    }

    public StringBuilder getZkTopicPath(StringBuilder sb, ByteString topic) {
        return getZkTopicsPrefix(sb).append("/").append(topic.toStringUtf8());
    }

    public StringBuilder getZkHostsPrefix(StringBuilder sb) {
        return getZkRegionPrefix(sb).append("/hosts");
    }

    public HedwigSocketAddress getServerAddr() {
        if (myServerAddress == null) {
            refreshMyServerAddress();
        }
        return myServerAddress;
    }

    /**
     * Return ZooKeeper list of servers. Default is localhost.
     * 
     * @return String
     */
    public String getZkHost() {
        List servers = conf.getList(ZK_HOST, null);
        if (null == servers || 0 == servers.size()) {
            return "localhost";
        }
        return StringUtils.join(servers, ",");
    }

    /**
     * Return ZooKeeper session timeout. Default is 2s.
     * 
     * @return int
     */
    public int getZkTimeout() {
        return conf.getInt(ZK_TIMEOUT, 2000);
    }

    /** 
     * Returns true if read-ahead enabled. Default is true.
     * 
     * @return boolean
     */
    public boolean getReadAheadEnabled() {
        return conf.getBoolean(READAHEAD_ENABLED, true)
            || conf.getBoolean("readhead_enabled");
        // the key was misspelt in a previous version, so compensate here
    }

    /**
     * Returns true if standalone. Default is false.
     * 
     * @return boolean
     */
    public boolean isStandalone() {
        return conf.getBoolean(STANDALONE, false);
    }

    /**
     * Returns list of regions. 
     * 
     * @return List<String>
     */
    public List<String> getRegions() {
        if (regionList == null) {
            refreshRegionList();
        }
        return regionList;
    }

    /**
     *  Returns the name of the SSL certificate if available as a resource.
     * 
     * @return String
     */
    public String getCertName() {
        return conf.getString(CERT_NAME, "");
    }

    /**
     * This is the path to the SSL certificate if it is available as a file.
     * 
     * @return String
     */
    public String getCertPath() {
        return conf.getString(CERT_PATH, "");
    }

    // This method return the SSL certificate as an InputStream based on if it
    // is configured to be available as a resource or as a file. If nothing is
    // configured correctly, then a ConfigurationException will be thrown as
    // we do not know how to obtain the SSL certificate stream.
    public InputStream getCertStream() throws FileNotFoundException, ConfigurationException {
        String certName = getCertName();
        String certPath = getCertPath();
        if (certName != null && !certName.isEmpty()) {
            return getClass().getResourceAsStream(certName);
        } else if (certPath != null && !certPath.isEmpty()) {
            return new FileInputStream(certPath);
        } else
            throw new ConfigurationException("SSL Certificate configuration does not have resource name or path set!");
    }

    /**
     * Returns the password used for BookKeeper ledgers. Default
     * is the empty string.
     * 
     * @return
     */
    public String getPassword() {
        return conf.getString(PASSWORD, "");
    }

    /**
     * Returns true if SSL is enabled. Default is false.
     * 
     * @return boolean
     */
    public boolean isSSLEnabled() {
        return conf.getBoolean(SSL_ENABLED, false);
    }

    /**
     * Gets the number of messages consumed before persisting
     * information about consumed messages. A value greater than
     * one avoids persisting information about consumed messages
     * upon every consumed message. Default is 50.
     * 
     * @return int
     */
    public int getConsumeInterval() {
        return conf.getInt(CONSUME_INTERVAL, 50);
    }

    /**
     * Returns the interval to release a topic. If this
     * parameter is greater than zero, then schedule a
     * task to release an owned topic. Default is 0 (never released).
     * 
     * @return int
     */
    public int getRetentionSecs() {
        return conf.getInt(RETENTION_SECS, 0);
    }

    /**
     * Specifies that the topic should be automatically released
     * once a fixed duration after the topic is owned, a message is
     * published, or a message is delivered.
     *
     * @return the length of time after an entry is last accessed that
     *         it should be automatically removed.
     */
    public int getRetentionSecsAfterAccess() {
        return conf.getInt(RETENTION_SECS_AFTER_ACCESS, 0);
    }

    /**
     * Max number of topics for a hub server to serve.
     *
     * @return max number of topics for a hub server to serve.
     */
    public int getMaxNumTopics() {
        return conf.getInt(MAX_NUM_TOPICS, Integer.MAX_VALUE);
    }

    /**
     * Minimum size of internal structure to store topics.
     *
     * @return init number of topics for a hub server.
     */
    public int getInitNumTopics() {
        return conf.getInt(INIT_NUM_TOPICS, 128);
    }

    /**
     * True if SSL is enabled across regions.
     * 
     * @return boolean
     */
    public boolean isInterRegionSSLEnabled() {
        return conf.getBoolean(INTER_REGION_SSL_ENABLED, false);
    }

    /**
     * This parameter is used to determine how often we run the 
     * SubscriptionManager's Messages Consumed timer task thread 
     * (in milliseconds).
     * 
     * @return int
     */
    public int getMessagesConsumedThreadRunInterval() {
        return conf.getInt(MESSAGES_CONSUMED_THREAD_RUN_INTERVAL, 60000);
    }

    /**
     * This parameter is used to determine how often we run a thread
     * to retry those failed remote subscriptions in asynchronous mode
     * (in milliseconds).
     * 
     * @return int
     */
    public int getRetryRemoteSubscribeThreadRunInterval() {
        return conf.getInt(RETRY_REMOTE_SUBSCRIBE_THREAD_RUN_INTERVAL, 120000);
    }

    /**
     * This parameter is for setting the default maximum number of messages which
     * can be delivered to a subscriber without being consumed.
     * we pause messages delivery to a subscriber when reaching the window size
     * 
     * @return int
     */
    public int getDefaultMessageWindowSize() {
        return conf.getInt(DEFAULT_MESSAGE_WINDOW_SIZE, 0);
    }

    /**
     * This parameter is used when Bookkeeper is the persistence
     * store and indicates what the ensemble size is (i.e. how
     * many bookie servers to stripe the ledger entries across).
     * 
     * @return int
     */
    public int getBkEnsembleSize() {
        return conf.getInt(BK_ENSEMBLE_SIZE, 3);
    }


    /**
     * This parameter is used when Bookkeeper is the persistence store
     * and indicates what the quorum size is (i.e. how many redundant
     * copies of each ledger entry is written).
     * 
     * @return int
     * @deprecated please use #getBkWriteQuorumSize() and #getBkAckQuorumSize()
     */
    @Deprecated
    protected int getBkQuorumSize() {
        return conf.getInt(BK_QUORUM_SIZE, 2);
    }

    /**
     * Get the write quorum size for BookKeeper client, which is used to
     * indicate how many redundant copies of each ledger entry is written.
     *
     * @return write quorum size for BookKeeper client.
     */
    public int getBkWriteQuorumSize() {
        if (conf.containsKey(BK_WRITE_QUORUM_SIZE)) {
            return conf.getInt(BK_WRITE_QUORUM_SIZE, 2);
        } else {
            return getBkQuorumSize();
        }
    }

    /**
     * Get the ack quorum size for BookKeeper client.
     *
     * @return ack quorum size for BookKeeper client.
     */
    public int getBkAckQuorumSize() {
        if (conf.containsKey(BK_ACK_QUORUM_SIZE)) {
            return conf.getInt(BK_ACK_QUORUM_SIZE, 2);
        } else {
            return getBkQuorumSize();
        }
    }

    /**
     * This parameter is used when BookKeeper is the persistence storage,
     * and indicates when the number of entries stored in a ledger reach
     * the threshold, hub server will open a new ledger to write.
     *
     * @return max entries per ledger
     */
    public long getMaxEntriesPerLedger() {
        return conf.getLong(MAX_ENTRIES_PER_LEDGER, 0L);
    }

    /*
     * Is this a valid configuration that we can run with? This code might grow
     * over time.
     */
    public void validate() throws ConfigurationException {
        if (!getZkPrefix().startsWith("/")) {
            throw new ConfigurationException(ZK_PREFIX + " must start with a /");
        }
        // Validate that if Regions exist and inter-region communication is SSL
        // enabled, that the Regions correspond to valid HedwigSocketAddresses,
        // namely that SSL ports are present.
        if (isInterRegionSSLEnabled() && getRegions().size() > 0) {
            for (String hubString : getRegions()) {
                HedwigSocketAddress hub = new HedwigSocketAddress(hubString);
                if (hub.getSSLSocketAddress() == null)
                    throw new ConfigurationException("Region defined does not have required SSL port: " + hubString);
            }
        }
        // Validate that the Bookkeeper ensemble size >= quorum size.
        if (getBkEnsembleSize() < getBkWriteQuorumSize()) {
            throw new ConfigurationException("BK ensemble size (" + getBkEnsembleSize()
                                             + ") is less than the write quorum size (" + getBkWriteQuorumSize() + ")");
        }

        if (getBkWriteQuorumSize() < getBkAckQuorumSize()) {
            throw new ConfigurationException("BK write quorum size (" + getBkWriteQuorumSize()
                                             + ") is less than the ack quorum size (" + getBkAckQuorumSize() + ")");
        }

        // add other checks here
    }

    /**
     * Get number of read ahead cache threads.
     *
     * @return number of read ahead cache threads.
     */
    public int getNumReadAheadCacheThreads() {
        return conf.getInt(NUM_READAHEAD_CACHE_THREADS, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Get number of delivery threads
     *
     * @return number of delivery threads.
     */
    public int getNumDeliveryThreads() {
        return conf.getInt(NUM_DELIVERY_THREADS, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Whether enable metadata manager based topic manager.
     *
     * @return true if enabled metadata manager based topic manager.
     */
    public boolean isMetadataManagerBasedTopicManagerEnabled() {
        return conf.getBoolean(METADATA_MANAGER_BASED_TOPIC_MANAGER_ENABLED, false);
    }

    /**
     * Get metadata manager factory class.
     *
     * @return manager class
     */
    public Class<? extends MetadataManagerFactory> getMetadataManagerFactoryClass()
    throws ConfigurationException {
        return ReflectionUtils.getClass(conf, METADATA_MANAGER_FACTORY_CLASS,
                                        null, MetadataManagerFactory.class,
                                        defaultLoader);
    }

    /**
     * Set metadata manager factory class name
     *
     * @param managerClsName
     *          Manager Class Name
     * @return server configuration
     */
    public ServerConfiguration setMetadataManagerFactoryName(String managerClsName) {
        conf.setProperty(METADATA_MANAGER_FACTORY_CLASS, managerClsName);
        return this;
    }

    /**
     * Get metastore implementation class.
     *
     * @return metastore implementation class name.
     */
    public String getMetastoreImplClass() {
        return conf.getString(METASTORE_IMPL_CLASS);
    }

    /**
     * Get max entries per scan in metastore.
     *
     * @return max entries per scan in metastore.
     */
    public int getMetastoreMaxEntriesPerScan() {
        return conf.getInt(METASTORE_MAX_ENTRIES_PER_SCAN, 50);
    }
}
