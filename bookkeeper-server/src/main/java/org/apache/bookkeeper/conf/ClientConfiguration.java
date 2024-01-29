/*
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.FEATURE_DISABLE_ENSEMBLE_CHANGE;

import io.netty.buffer.ByteBuf;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.api.BookKeeperBuilder;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.ZKRegistrationClient;
import org.apache.bookkeeper.replication.Auditor;
import org.apache.commons.configuration.ConfigurationException;


/**
 * Configuration settings for client side.
 */
public class ClientConfiguration extends AbstractConfiguration<ClientConfiguration> {

    // Throttle value
    protected static final String THROTTLE = "throttle";

    // Digest Type
    protected static final String DIGEST_TYPE = "digestType";
    protected static final String ENABLE_DIGEST_TYPE_AUTODETECTION = "enableDigestTypeAutodetection";

    // Passwd
    protected static final String PASSWD = "passwd";

    // Client TLS (@deprecated since 4.7.0)
    /**
     * @deprecated Use {@link #TLS_KEYSTORE_TYPE}
     */
    @Deprecated
    protected static final String CLIENT_TLS_KEYSTORE_TYPE = "clientKeyStoreType";

    /**
     * @deprecated Use {@link #TLS_KEYSTORE}
     */
    @Deprecated
    protected static final String CLIENT_TLS_KEYSTORE = "clientKeyStore";

    /**
     * @deprecated Use {@link #TLS_KEYSTORE_PASSWORD_PATH}
     */
    @Deprecated
    protected static final String CLIENT_TLS_KEYSTORE_PASSWORD_PATH = "clientKeyStorePasswordPath";

    /**
     * @deprecated Use {@link #TLS_TRUSTSTORE_TYPE}
     */
    @Deprecated
    protected static final String CLIENT_TLS_TRUSTSTORE_TYPE = "clientTrustStoreType";

    /**
     * @deprecated Use {@link #TLS_TRUSTSTORE}
     */
    @Deprecated
    protected static final String CLIENT_TLS_TRUSTSTORE = "clientTrustStore";

    /**
     * @deprecated Use {@link #TLS_TRUSTSTORE_PASSWORD_PATH}
     */
    @Deprecated
    protected static final String CLIENT_TLS_TRUSTSTORE_PASSWORD_PATH = "clientTrustStorePasswordPath";

    // NIO Parameters
    protected static final String CLIENT_TCP_NODELAY = "clientTcpNoDelay";
    protected static final String CLIENT_SOCK_KEEPALIVE = "clientSockKeepalive";
    protected static final String CLIENT_SENDBUFFER_SIZE = "clientSendBufferSize";
    protected static final String CLIENT_RECEIVEBUFFER_SIZE = "clientReceiveBufferSize";
    protected static final String CLIENT_WRITEBUFFER_LOW_WATER_MARK = "clientWriteBufferLowWaterMark";
    protected static final String CLIENT_WRITEBUFFER_HIGH_WATER_MARK = "clientWriteBufferHighWaterMark";
    protected static final String CLIENT_CONNECT_TIMEOUT_MILLIS = "clientConnectTimeoutMillis";
    protected static final String CLIENT_TCP_USER_TIMEOUT_MILLIS = "clientTcpUserTimeoutMillis";
    protected static final String NUM_CHANNELS_PER_BOOKIE = "numChannelsPerBookie";
    protected static final String USE_V2_WIRE_PROTOCOL = "useV2WireProtocol";
    protected static final String NETTY_USE_POOLED_BUFFERS = "nettyUsePooledBuffers";

    // Read Parameters
    protected static final String READ_TIMEOUT = "readTimeout";
    protected static final String SPECULATIVE_READ_TIMEOUT = "speculativeReadTimeout";
    protected static final String FIRST_SPECULATIVE_READ_TIMEOUT = "firstSpeculativeReadTimeout";
    protected static final String MAX_SPECULATIVE_READ_TIMEOUT = "maxSpeculativeReadTimeout";
    protected static final String SPECULATIVE_READ_TIMEOUT_BACKOFF_MULTIPLIER =
        "speculativeReadTimeoutBackoffMultiplier";
    protected static final String FIRST_SPECULATIVE_READ_LAC_TIMEOUT = "firstSpeculativeReadLACTimeout";
    protected static final String MAX_SPECULATIVE_READ_LAC_TIMEOUT = "maxSpeculativeReadLACTimeout";
    protected static final String SPECULATIVE_READ_LAC_TIMEOUT_BACKOFF_MULTIPLIER =
        "speculativeReadLACTimeoutBackoffMultiplier";
    protected static final String ENABLE_PARALLEL_RECOVERY_READ = "enableParallelRecoveryRead";
    protected static final String RECOVERY_READ_BATCH_SIZE = "recoveryReadBatchSize";
    protected static final String REORDER_READ_SEQUENCE_ENABLED = "reorderReadSequenceEnabled";
    protected static final String STICKY_READS_ENABLED = "stickyReadSEnabled";
    // Add Parameters
    protected static final String OPPORTUNISTIC_STRIPING = "opportunisticStriping";
    protected static final String DELAY_ENSEMBLE_CHANGE = "delayEnsembleChange";
    protected static final String MAX_ALLOWED_ENSEMBLE_CHANGES = "maxNumEnsembleChanges";
    // Timeout Setting
    protected static final String ADD_ENTRY_TIMEOUT_SEC = "addEntryTimeoutSec";
    protected static final String ADD_ENTRY_QUORUM_TIMEOUT_SEC = "addEntryQuorumTimeoutSec";
    protected static final String READ_ENTRY_TIMEOUT_SEC = "readEntryTimeoutSec";
    protected static final String TIMEOUT_MONITOR_INTERVAL_SEC = "timeoutMonitorIntervalSec";
    protected static final String TIMEOUT_TASK_INTERVAL_MILLIS = "timeoutTaskIntervalMillis";
    protected static final String EXPLICIT_LAC_INTERVAL = "explicitLacInterval";
    protected static final String PCBC_TIMEOUT_TIMER_TICK_DURATION_MS = "pcbcTimeoutTimerTickDurationMs";
    protected static final String PCBC_TIMEOUT_TIMER_NUM_TICKS = "pcbcTimeoutTimerNumTicks";
    protected static final String TIMEOUT_TIMER_TICK_DURATION_MS = "timeoutTimerTickDurationMs";
    protected static final String TIMEOUT_TIMER_NUM_TICKS = "timeoutTimerNumTicks";
    // backpressure configuration
    protected static final String WAIT_TIMEOUT_ON_BACKPRESSURE = "waitTimeoutOnBackpressureMs";

    // Bookie health check settings
    protected static final String BOOKIE_HEALTH_CHECK_ENABLED = "bookieHealthCheckEnabled";
    protected static final String BOOKIE_HEALTH_CHECK_INTERVAL_SECONDS = "bookieHealthCheckIntervalSeconds";
    protected static final String BOOKIE_ERROR_THRESHOLD_PER_INTERVAL = "bookieErrorThresholdPerInterval";
    protected static final String BOOKIE_QUARANTINE_TIME_SECONDS = "bookieQuarantineTimeSeconds";
    protected static final String BOOKIE_QUARANTINE_RATIO = "bookieQuarantineRatio";

    // Bookie info poll interval
    protected static final String DISK_WEIGHT_BASED_PLACEMENT_ENABLED = "diskWeightBasedPlacementEnabled";
    protected static final String GET_BOOKIE_INFO_INTERVAL_SECONDS = "getBookieInfoIntervalSeconds";
    protected static final String GET_BOOKIE_INFO_RETRY_INTERVAL_SECONDS = "getBookieInfoRetryIntervalSeconds";
    protected static final String BOOKIE_MAX_MULTIPLE_FOR_WEIGHTED_PLACEMENT =
        "bookieMaxMultipleForWeightBasedPlacement";
    protected static final String GET_BOOKIE_INFO_TIMEOUT_SECS = "getBookieInfoTimeoutSecs";
    protected static final String START_TLS_TIMEOUT_SECS = "startTLSTimeoutSecs";
    protected static final String TLS_HOSTNAME_VERIFICATION_ENABLED = "tlsHostnameVerificationEnabled";

    // Number of Threads
    protected static final String NUM_WORKER_THREADS = "numWorkerThreads";
    protected static final String NUM_IO_THREADS = "numIOThreads";

    // Ensemble Placement Policy
    public static final String ENSEMBLE_PLACEMENT_POLICY = "ensemblePlacementPolicy";
    protected static final String NETWORK_TOPOLOGY_STABILIZE_PERIOD_SECONDS = "networkTopologyStabilizePeriodSeconds";
    protected static final String READ_REORDER_THRESHOLD_PENDING_REQUESTS = "readReorderThresholdPendingRequests";
    protected static final String ENSEMBLE_PLACEMENT_POLICY_ORDER_SLOW_BOOKIES =
        "ensemblePlacementPolicyOrderSlowBookies";
    protected static final String BOOKIE_ADDRESS_RESOLVER_ENABLED = "bookieAddressResolverEnabled";
    // Use hostname to resolve local placement info
    public static final String USE_HOSTNAME_RESOLVE_LOCAL_NODE_PLACEMENT_POLICY =
        "useHostnameResolveLocalNodePlacementPolicy";

    // Stats
    protected static final String ENABLE_TASK_EXECUTION_STATS = "enableTaskExecutionStats";
    protected static final String TASK_EXECUTION_WARN_TIME_MICROS = "taskExecutionWarnTimeMicros";

    // Failure History Settings
    protected static final String ENABLE_BOOKIE_FAILURE_TRACKING = "enableBookieFailureTracking";
    protected static final String BOOKIE_FAILURE_HISTORY_EXPIRATION_MS = "bookieFailureHistoryExpirationMSec";

    // Discovery
    protected static final String FOLLOW_BOOKIE_ADDRESS_TRACKING = "enableBookieAddressTracking";

    // Names of dynamic features
    protected static final String DISABLE_ENSEMBLE_CHANGE_FEATURE_NAME = "disableEnsembleChangeFeatureName";

    // Role of the client
    protected static final String CLIENT_ROLE = "clientRole";

    /**
     * This client will act as a standard client.
     */
    public static final String CLIENT_ROLE_STANDARD = "standard";

    /**
     * This client will act as a system client, like the {@link Auditor}.
     */
    public static final String CLIENT_ROLE_SYSTEM = "system";

    // Client auth provider factory class name. It must be configured on Bookies to for the Auditor
    protected static final String CLIENT_AUTH_PROVIDER_FACTORY_CLASS = "clientAuthProviderFactoryClass";

    // Registration Client
    protected static final String REGISTRATION_CLIENT_CLASS = "registrationClientClass";

    // Logs
    protected static final String CLIENT_CONNECT_BOOKIE_UNAVAILABLE_LOG_THROTTLING =
            "clientConnectBookieUnavailableLogThrottling";

    //For batch read api, it the batch read is not stable, we can fail back to single read by this config.
    protected static final String BATCH_READ_ENABLED = "batchReadEnabled";

    /**
     * Construct a default client-side configuration.
     */
    public ClientConfiguration() {
        super();
    }

    /**
     * Construct a client-side configuration using a base configuration.
     *
     * @param conf
     *          Base configuration
     */
    public ClientConfiguration(AbstractConfiguration conf) {
        super();
        loadConf(conf);
    }

    /**
     * Get throttle value.
     *
     * @return throttle value
     * @see #setThrottleValue
     */
    public int getThrottleValue() {
        return this.getInt(THROTTLE, 5000);
    }

    /**
     * Set throttle value.
     *
     * <p>Since BookKeeper process requests in asynchronous way, it will holds
     * those pending request in queue. You may easily run it out of memory
     * if producing too many requests than the capability of bookie servers can handle.
     * To prevent that from happening, you can set a throttle value here.
     *
     * <p>Setting the throttle value to 0, will disable any throttling.
     *
     * @param throttle
     *          Throttle Value
     * @return client configuration
     */
    public ClientConfiguration setThrottleValue(int throttle) {
        this.setProperty(THROTTLE, Integer.toString(throttle));
        return this;
    }

    /**
     * Get autodetection of digest type.
     *
     * <p>Ignores provided digestType, if enabled and uses one from ledger metadata instead.
     * Incompatible with ledger created by bookie versions &lt; 4.2
     *
     * <p>It is turned on by default since 4.7.
     *
     * @return flag to enable/disable autodetection of digest type.
     */
    public boolean getEnableDigestTypeAutodetection() {
        return getBoolean(ENABLE_DIGEST_TYPE_AUTODETECTION, true);
    }

    /**
     * Enable autodetection of digest type.
     * Ignores provided digestType, if enabled and uses one from ledger metadata instead.
     * Incompatible with ledger created by bookie versions &lt; 4.2
     *
     * @return client configuration.
     */
    public ClientConfiguration setEnableDigestTypeAutodetection(boolean enable) {
        this.setProperty(ENABLE_DIGEST_TYPE_AUTODETECTION, enable);
        return this;
    }

    /**
     * Get digest type used in bookkeeper admin.
     *
     * @return digest type
     * @see #setBookieRecoveryDigestType
     */
    public DigestType getBookieRecoveryDigestType() {
        return DigestType.valueOf(this.getString(DIGEST_TYPE, DigestType.CRC32.toString()));
    }

    /**
     * Set digest type used in bookkeeper admin.
     *
     * <p>Digest Type and Passwd used to open ledgers for admin tool
     * For now, assume that all ledgers were created with the same DigestType
     * and password. In the future, this admin tool will need to know for each
     * ledger, what was the DigestType and password used to create it before it
     * can open it. These values will come from System properties, though fixed
     * defaults are defined here.
     *
     * @param digestType
     *          Digest Type
     * @return client configuration
     */
    public ClientConfiguration setBookieRecoveryDigestType(DigestType digestType) {
        this.setProperty(DIGEST_TYPE, digestType.toString());
        return this;
    }

    /**
     * Get passwd used in bookkeeper admin.
     *
     * @return password
     * @see #setBookieRecoveryPasswd
     */
    public byte[] getBookieRecoveryPasswd() {
        return this.getString(PASSWD, "").getBytes(UTF_8);
    }

    /**
     * Set passwd used in bookkeeper admin.
     *
     * <p>Digest Type and Passwd used to open ledgers for admin tool
     * For now, assume that all ledgers were created with the same DigestType
     * and password. In the future, this admin tool will need to know for each
     * ledger, what was the DigestType and password used to create it before it
     * can open it. These values will come from System properties, though fixed
     * defaults are defined here.
     *
     * @param passwd
     *          Password
     * @return client configuration
     */
    public ClientConfiguration setBookieRecoveryPasswd(byte[] passwd) {
        setProperty(PASSWD, new String(passwd, UTF_8));
        return this;
    }

    /**
     * Is tcp connection no delay.
     *
     * @return tcp socket nodelay setting
     * @see #setClientTcpNoDelay
     */
    public boolean getClientTcpNoDelay() {
        return getBoolean(CLIENT_TCP_NODELAY, true);
    }

    /**
     * Set socket nodelay setting.
     *
     * <p>This settings is used to enabled/disabled Nagle's algorithm, which is a means of
     * improving the efficiency of TCP/IP networks by reducing the number of packets
     * that need to be sent over the network. If you are sending many small messages,
     * such that more than one can fit in a single IP packet, setting client.tcpnodelay
     * to false to enable Nagle algorithm can provide better performance.
     * <br>
     * Default value is true.
     *
     * @param noDelay
     *          NoDelay setting
     * @return client configuration
     */
    public ClientConfiguration setClientTcpNoDelay(boolean noDelay) {
        setProperty(CLIENT_TCP_NODELAY, Boolean.toString(noDelay));
        return this;
    }

    /**
     * get socket keepalive.
     *
     * @return socket keepalive setting
     */
    public boolean getClientSockKeepalive() {
        return getBoolean(CLIENT_SOCK_KEEPALIVE, true);
    }

    /**
     * Set socket keepalive setting.
     *
     * <p>This setting is used to send keep-alive messages on connection-oriented sockets.
     *
     * @param keepalive
     *            KeepAlive setting
     * @return client configuration
     */
    public ClientConfiguration setClientSockKeepalive(boolean keepalive) {
        setProperty(CLIENT_SOCK_KEEPALIVE, Boolean.toString(keepalive));
        return this;
    }

    /**
     * Get client netty channel send buffer size.
     *
     * @return client netty channel send buffer size
     */
    public int getClientSendBufferSize() {
        return getInt(CLIENT_SENDBUFFER_SIZE, 1 * 1024 * 1024);
    }

    /**
     * Set client netty channel send buffer size.
     *
     * @param bufferSize
     *          client netty channel send buffer size.
     * @return client configuration.
     */
    public ClientConfiguration setClientSendBufferSize(int bufferSize) {
        setProperty(CLIENT_SENDBUFFER_SIZE, bufferSize);
        return this;
    }

    /**
     * Get client netty channel receive buffer size.
     *
     * @return client netty channel receive buffer size.
     */
    public int getClientReceiveBufferSize() {
        return getInt(CLIENT_RECEIVEBUFFER_SIZE, 1 * 1024 * 1024);
    }

    /**
     * Set client netty channel receive buffer size.
     *
     * @param bufferSize
     *          netty channel receive buffer size.
     * @return client configuration.
     */
    public ClientConfiguration setClientReceiveBufferSize(int bufferSize) {
        setProperty(CLIENT_RECEIVEBUFFER_SIZE, bufferSize);
        return this;
    }

    /**
     * Get client netty channel write buffer low water mark.
     *
     * @return netty channel write buffer low water mark.
     */
    public int getClientWriteBufferLowWaterMark() {
        return getInt(CLIENT_WRITEBUFFER_LOW_WATER_MARK, 384 * 1024);
    }

    /**
     * Set client netty channel write buffer low water mark.
     *
     * @param waterMark
     *          netty channel write buffer low water mark.
     * @return client configuration.
     */
    public ClientConfiguration setClientWriteBufferLowWaterMark(int waterMark) {
        setProperty(CLIENT_WRITEBUFFER_LOW_WATER_MARK, waterMark);
        return this;
    }

    /**
     * Get client netty channel write buffer high water mark.
     *
     * @return netty channel write buffer high water mark.
     */
    public int getClientWriteBufferHighWaterMark() {
        return getInt(CLIENT_WRITEBUFFER_HIGH_WATER_MARK, 512 * 1024);
    }

    /**
     * Set client netty channel write buffer high water mark.
     *
     * @param waterMark
     *          netty channel write buffer high water mark.
     * @return client configuration.
     */
    public ClientConfiguration setClientWriteBufferHighWaterMark(int waterMark) {
        setProperty(CLIENT_WRITEBUFFER_HIGH_WATER_MARK, waterMark);
        return this;
    }

    /**
     * Get the tick duration in milliseconds that used for timeout timer.
     *
     * @return tick duration in milliseconds
     */
    public long getTimeoutTimerTickDurationMs() {
        return getLong(TIMEOUT_TIMER_TICK_DURATION_MS, 100);
    }

    /**
     * Set the tick duration in milliseconds that used for timeout timer.
     *
     * @param tickDuration
     *          tick duration in milliseconds.
     * @return client configuration.
     */
    public ClientConfiguration setTimeoutTimerTickDurationMs(long tickDuration) {
        setProperty(TIMEOUT_TIMER_TICK_DURATION_MS, tickDuration);
        return this;
    }

    /**
     * Get number of ticks that used for timeout timer.
     *
     * @return number of ticks that used for timeout timer.
     */
    public int getTimeoutTimerNumTicks() {
        return getInt(TIMEOUT_TIMER_NUM_TICKS, 1024);
    }

    /**
     * Set number of ticks that used for timeout timer.
     *
     * @param numTicks
     *          number of ticks that used for timeout timer.
     * @return client configuration.
     */
    public ClientConfiguration setTimeoutTimerNumTicks(int numTicks) {
        setProperty(TIMEOUT_TIMER_NUM_TICKS, numTicks);
        return this;
    }

    /**
     * Get client netty connect timeout in millis.
     *
     * @return client netty connect timeout in millis.
     */
    public int getClientConnectTimeoutMillis() {
        // 10 seconds as netty default value.
        return getInt(CLIENT_CONNECT_TIMEOUT_MILLIS, 10000);
    }

    /**
     * Set client netty connect timeout in millis.
     *
     * @param connectTimeoutMillis
     *          client netty connect timeout in millis.
     * @return client configuration.
     */
    public ClientConfiguration setClientConnectTimeoutMillis(int connectTimeoutMillis) {
        setProperty(CLIENT_CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis);
        return this;
    }

    /**
     * Get client netty TCP user timeout in millis (only for Epoll channels).
     *
     * @return client netty Epoll user tcp timeout in millis.
     * @throws NoSuchElementException if the property is not set.
     */
    public int getTcpUserTimeoutMillis() {
        return getInt(CLIENT_TCP_USER_TIMEOUT_MILLIS);
    }

    /**
     * Set client netty TCP user timeout in millis (only for Epoll channels).
     *
     * @param tcpUserTimeoutMillis
     *          client netty TCP user timeout in millis.
     * @return client configuration.
     */
    public ClientConfiguration setTcpUserTimeoutMillis(int tcpUserTimeoutMillis) {
        setProperty(CLIENT_TCP_USER_TIMEOUT_MILLIS, tcpUserTimeoutMillis);
        return this;
    }

    /**
     * Get num channels per bookie.
     *
     * @return num channels per bookie.
     */
    public int getNumChannelsPerBookie() {
        return getInt(NUM_CHANNELS_PER_BOOKIE, 1);
    }

    /**
     * Set num channels per bookie.
     *
     * @param numChannelsPerBookie
     *          num channels per bookie.
     * @return client configuration.
     */
    public ClientConfiguration setNumChannelsPerBookie(int numChannelsPerBookie) {
        setProperty(NUM_CHANNELS_PER_BOOKIE, numChannelsPerBookie);
        return this;
    }

    /**
     * Use older Bookkeeper wire protocol (no protobuf).
     *
     * @return whether or not to use older Bookkeeper wire protocol (no protobuf)
     */
    public boolean getUseV2WireProtocol() {
        return getBoolean(USE_V2_WIRE_PROTOCOL, false);
    }

    /**
     * Set whether or not to use older Bookkeeper wire protocol (no protobuf).
     *
     * @param useV2WireProtocol
     *          whether or not to use older Bookkeeper wire protocol (no protobuf)
     * @return client configuration.
     */
    public ClientConfiguration setUseV2WireProtocol(boolean useV2WireProtocol) {
        setProperty(USE_V2_WIRE_PROTOCOL, useV2WireProtocol);
        return this;
    }

    /**
     * Get the socket read timeout. This is the number of
     * seconds we wait without hearing a response from a bookie
     * before we consider it failed.
     *
     * <p>The default is 5 seconds.
     *
     * @return the current read timeout in seconds
     * @deprecated use {@link #getReadEntryTimeout()} or {@link #getAddEntryTimeout()} instead
     */
    @Deprecated
    public int getReadTimeout() {
        return getInt(READ_TIMEOUT, 5);
    }

    /**
     * Set the socket read timeout.
     * @see #getReadTimeout()
     * @param timeout The new read timeout in seconds
     * @return client configuration
     * @deprecated use {@link #setReadEntryTimeout(int)} or {@link #setAddEntryTimeout(int)} instead
     */
    @Deprecated
    public ClientConfiguration setReadTimeout(int timeout) {
        setProperty(READ_TIMEOUT, Integer.toString(timeout));
        return this;
    }

    /**
     * Get the timeout for add request. This is the number of seconds we wait without hearing
     * a response for add request from a bookie before we consider it failed.
     *
     * <p>The default value is 5 second for backwards compatibility.
     *
     * @return add entry timeout.
     */
    @SuppressWarnings("deprecation")
    public int getAddEntryTimeout() {
        return getInt(ADD_ENTRY_TIMEOUT_SEC, getReadTimeout());
    }

    /**
     * Set timeout for add entry request.
     * @see #getAddEntryTimeout()
     *
     * @param timeout
     *          The new add entry timeout in seconds.
     * @return client configuration.
     */
    public ClientConfiguration setAddEntryTimeout(int timeout) {
        setProperty(ADD_ENTRY_TIMEOUT_SEC, timeout);
        return this;
    }

    /**
     * Get the timeout for top-level add request. That is, the amount of time we should spend
     * waiting for ack quorum.
     *
     * @return add entry ack quorum timeout.
     */
    public int getAddEntryQuorumTimeout() {
        return getInt(ADD_ENTRY_QUORUM_TIMEOUT_SEC, -1);
    }

    /**
     * Set timeout for top-level add entry request.
     * @see #getAddEntryQuorumTimeout()
     *
     * @param timeout
     *          The new add entry ack quorum timeout in seconds.
     * @return client configuration.
     */
    public ClientConfiguration setAddEntryQuorumTimeout(int timeout) {
        setProperty(ADD_ENTRY_QUORUM_TIMEOUT_SEC, timeout);
        return this;
    }

    /**
     * Get the timeout for read entry. This is the number of seconds we wait without hearing
     * a response for read entry request from a bookie before we consider it failed. By default,
     * we use socket timeout specified at {@link #getReadTimeout()}.
     *
     * @return read entry timeout.
     */
    @SuppressWarnings("deprecation")
    public int getReadEntryTimeout() {
        return getInt(READ_ENTRY_TIMEOUT_SEC, getReadTimeout());
    }

    /**
     * Set the timeout for read entry request.
     * @see #getReadEntryTimeout()
     *
     * @param timeout
     *          The new read entry timeout in seconds.
     * @return client configuration.
     */
    public ClientConfiguration setReadEntryTimeout(int timeout) {
        setProperty(READ_ENTRY_TIMEOUT_SEC, timeout);
        return this;
    }

    /**
     * Get the interval between successive executions of the operation timeout monitor. This value is in seconds.
     *
     * @see #setTimeoutMonitorIntervalSec(long)
     * @return the interval at which request timeouts will be checked
     */
    public long getTimeoutMonitorIntervalSec() {
        int minTimeout = Math.min(Math.min(getAddEntryQuorumTimeout(),
                                           getAddEntryTimeout()), getReadEntryTimeout());
        return getLong(TIMEOUT_MONITOR_INTERVAL_SEC, Math.max(minTimeout / 2, 1));
    }

    /**
     * Set the interval between successive executions of the operation timeout monitor. The value in seconds.
     * Every X seconds, all outstanding add and read operations are checked to see if they have been running
     * for longer than their configured timeout. Any that have been will be errored out.
     *
     * <p>This timeout should be set to a value which is a fraction of the values of
     * {@link #getAddEntryQuorumTimeout}, {@link #getAddEntryTimeout} and {@link #getReadEntryTimeout},
     * so that these timeouts run in a timely fashion.
     *
     * @param timeoutInterval The timeout monitor interval, in seconds
     * @return client configuration
     */
    public ClientConfiguration setTimeoutMonitorIntervalSec(long timeoutInterval) {
        setProperty(TIMEOUT_MONITOR_INTERVAL_SEC, Long.toString(timeoutInterval));
        return this;
    }

    /**
     * Get the interval between successive executions of the PerChannelBookieClient's TimeoutTask. This value is in
     * milliseconds. Every X milliseconds, the timeout task will be executed and it will error out entries that have
     * timed out.
     *
     * <p>We do it more aggressive to not accumulate pending requests due to slow responses.
     *
     * @return the interval at which request timeouts will be checked
     */
    @Deprecated
    public long getTimeoutTaskIntervalMillis() {
        return getLong(TIMEOUT_TASK_INTERVAL_MILLIS,
                TimeUnit.SECONDS.toMillis(Math.min(getAddEntryTimeout(), getReadEntryTimeout())) / 2);
    }

    @Deprecated
    public ClientConfiguration setTimeoutTaskIntervalMillis(long timeoutMillis) {
        setProperty(TIMEOUT_TASK_INTERVAL_MILLIS, Long.toString(timeoutMillis));
        return this;
    }

    /**
     * Get the configured interval between  explicit LACs to bookies.
     * Generally LACs are piggy-backed on writes, and user can configure
     * the interval between these protocol messages. A value of '0' disables
     * sending any explicit LACs.
     *
     * @return interval between explicit LACs
     */
    public int getExplictLacInterval() {
        return getInt(EXPLICIT_LAC_INTERVAL, 0);
    }

    /**
     * Set the interval to check the need for sending an explicit LAC.
     * @param interval
     *        Number of milli seconds between checking the need for sending an explict LAC.
     * @return Client configuration.
     */
    public ClientConfiguration setExplictLacInterval(int interval) {
        setProperty(EXPLICIT_LAC_INTERVAL, interval);
        return this;
    }

    /**
     * Get the tick duration in milliseconds that used for the
     * HashedWheelTimer that used by PCBC to timeout
     * requests.
     *
     * @return tick duration in milliseconds
     */
    @Deprecated
    public long getPCBCTimeoutTimerTickDurationMs() {
        return getLong(PCBC_TIMEOUT_TIMER_TICK_DURATION_MS, 100);
    }

    /**
     * Set the tick duration in milliseconds that used for
     * HashedWheelTimer that used by PCBC to timeout
     * requests. Be aware of HashedWheelTimer if you
     * are going to modify this setting.
     *
     * @see #getPCBCTimeoutTimerTickDurationMs()
     *
     * @param tickDuration
     *          tick duration in milliseconds.
     * @return client configuration.
     */
    @Deprecated
    public ClientConfiguration setPCBCTimeoutTimerTickDurationMs(long tickDuration) {
        setProperty(PCBC_TIMEOUT_TIMER_TICK_DURATION_MS, tickDuration);
        return this;
    }

    /**
     * Get number of ticks that used for
     * HashedWheelTimer that used by PCBC to timeout
     * requests.
     *
     * @return number of ticks that used for timeout timer.
     */
    @Deprecated
    public int getPCBCTimeoutTimerNumTicks() {
        return getInt(PCBC_TIMEOUT_TIMER_NUM_TICKS, 1024);
    }

    /**
     * Set number of ticks that used for
     * HashedWheelTimer that used by PCBC to timeout request.
     * Be aware of HashedWheelTimer if you are going to modify
     * this setting.
     *
     * @see #getPCBCTimeoutTimerNumTicks()
     *
     * @param numTicks
     *          number of ticks that used for timeout timer.
     * @return client configuration.
     */
    @Deprecated
    public ClientConfiguration setPCBCTimeoutTimerNumTicks(int numTicks) {
        setProperty(PCBC_TIMEOUT_TIMER_NUM_TICKS, numTicks);
        return this;
    }

    /**
     * Timeout controlling wait on request send in case of unresponsive bookie(s)
     * (i.e. bookie in long GC etc.)
     *
     * @return timeout value
     *        negative value disables the feature
     *        0 to allow request to fail immediately
     *        Default is -1 (disabled)
     */
    public long getWaitTimeoutOnBackpressureMillis() {
        return getLong(WAIT_TIMEOUT_ON_BACKPRESSURE, -1);
    }

    /**
     * Timeout controlling wait on request send in case of unresponsive bookie(s)
     * (i.e. bookie in long GC etc.)
     *
     * @param value
     *        negative value disables the feature
     *        0 to allow request to fail immediately
     *        Default is -1 (disabled)
     * @return client configuration.
     */
    public ClientConfiguration setWaitTimeoutOnBackpressureMillis(long value) {
        setProperty(WAIT_TIMEOUT_ON_BACKPRESSURE, value);
        return this;
    }

    /**
     * Get the number of worker threads. This is the number of
     * worker threads used by bookkeeper client to submit operations.
     *
     * @return the number of worker threads
     */
    public int getNumWorkerThreads() {
        return getInt(NUM_WORKER_THREADS, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Set the number of worker threads.
     *
     * <p>
     * NOTE: setting the number of worker threads after BookKeeper object is constructed
     * will not take any effect on the number of threads in the pool.
     * </p>
     *
     * @see #getNumWorkerThreads()
     * @param numThreads number of worker threads used for bookkeeper
     * @return client configuration
     */
    public ClientConfiguration setNumWorkerThreads(int numThreads) {
        setProperty(NUM_WORKER_THREADS, numThreads);
        return this;
    }

    /**
     * Get the number of IO threads. This is the number of
     * threads used by Netty to handle TCP connections.
     *
     * @return the number of IO threads
     */
    public int getNumIOThreads() {
        return getInt(NUM_IO_THREADS, 2 * Runtime.getRuntime().availableProcessors());
    }

    /**
     * Set the number of IO threads.
     *
     * <p>
     * This is the number of threads used by Netty to handle TCP connections.
     * </p>
     *
     * <p>
     * NOTE: setting the number of IO threads after BookKeeper object is constructed
     * will not take any effect on the number of threads in the pool.
     * </p>
     *
     * @see #getNumIOThreads()
     * @param numThreads number of IO threads used for bookkeeper
     * @return client configuration
     */
    public ClientConfiguration setNumIOThreads(int numThreads) {
        setProperty(NUM_IO_THREADS, numThreads);
        return this;
    }

    /**
     * Get the period of time after which a speculative entry read should be triggered.
     * A speculative entry read is sent to the next replica bookie before
     * an error or response has been received for the previous entry read request.
     *
     * <p>A speculative entry read is only sent if we have not heard from the current
     * replica bookie during the entire read operation which may comprise of many entries.
     *
     * <p>Speculative reads allow the client to avoid having to wait for the connect timeout
     * in the case that a bookie has failed. It induces higher load on the network and on
     * bookies. This should be taken into account before changing this configuration value.
     *
     * @see org.apache.bookkeeper.client.LedgerHandle#asyncReadEntries
     * @return the speculative read timeout in milliseconds. Default 2000.
     */
    public int getSpeculativeReadTimeout() {
        return getInt(SPECULATIVE_READ_TIMEOUT, 2000);
    }

    /**
     * Set the speculative read timeout. A lower timeout will reduce read latency in the
     * case of a failed bookie, while increasing the load on bookies and the network.
     *
     * <p>The default is 2000 milliseconds. A value of 0 will disable speculative reads
     * completely.
     *
     * @see #getSpeculativeReadTimeout()
     * @param timeout the timeout value, in milliseconds
     * @return client configuration
     */
    public ClientConfiguration setSpeculativeReadTimeout(int timeout) {
        setProperty(SPECULATIVE_READ_TIMEOUT, timeout);
        return this;
    }

    /**
     * Get the first speculative read timeout.
     *
     * @return first speculative read timeout.
     */
    public int getFirstSpeculativeReadTimeout() {
        return getInt(FIRST_SPECULATIVE_READ_TIMEOUT, getSpeculativeReadTimeout());
    }

    /**
     * Set the first speculative read timeout.
     *
     * @param timeout
     *          first speculative read timeout.
     * @return client configuration.
     */
    public ClientConfiguration setFirstSpeculativeReadTimeout(int timeout) {
        setProperty(FIRST_SPECULATIVE_READ_TIMEOUT, timeout);
        return this;
    }

    /**
     * Multipler to use when determining time between successive speculative read requests.
     *
     * @return speculative read timeout backoff multiplier.
     */
    public float getSpeculativeReadTimeoutBackoffMultiplier() {
        return getFloat(SPECULATIVE_READ_TIMEOUT_BACKOFF_MULTIPLIER, 2.0f);
    }

    /**
     * Set the multipler to use when determining time between successive speculative read requests.
     *
     * @param speculativeReadTimeoutBackoffMultiplier
     *          multipler to use when determining time between successive speculative read requests.
     * @return client configuration.
     */
    public ClientConfiguration setSpeculativeReadTimeoutBackoffMultiplier(
            float speculativeReadTimeoutBackoffMultiplier) {
        setProperty(SPECULATIVE_READ_TIMEOUT_BACKOFF_MULTIPLIER, speculativeReadTimeoutBackoffMultiplier);
        return this;
    }

    /**
     * Multipler to use when determining time between successive speculative read LAC requests.
     *
     * @return speculative read LAC timeout backoff multiplier.
     */
    public float getSpeculativeReadLACTimeoutBackoffMultiplier() {
        return getFloat(SPECULATIVE_READ_LAC_TIMEOUT_BACKOFF_MULTIPLIER, 2.0f);
    }

    /**
     * Set the multipler to use when determining time between successive speculative read LAC requests.
     *
     * @param speculativeReadLACTimeoutBackoffMultiplier
     *          multipler to use when determining time between successive speculative read LAC requests.
     * @return client configuration.
     */
    public ClientConfiguration setSpeculativeReadLACTimeoutBackoffMultiplier(
            float speculativeReadLACTimeoutBackoffMultiplier) {
        setProperty(SPECULATIVE_READ_LAC_TIMEOUT_BACKOFF_MULTIPLIER, speculativeReadLACTimeoutBackoffMultiplier);
        return this;
    }

    /**
     * Get the max speculative read timeout.
     *
     * @return max speculative read timeout.
     */
    public int getMaxSpeculativeReadTimeout() {
        return getInt(MAX_SPECULATIVE_READ_TIMEOUT, getSpeculativeReadTimeout());
    }

    /**
     * Set the max speculative read timeout.
     *
     * @param timeout
     *          max speculative read timeout.
     * @return client configuration.
     */
    public ClientConfiguration setMaxSpeculativeReadTimeout(int timeout) {
        setProperty(MAX_SPECULATIVE_READ_TIMEOUT, timeout);
        return this;
    }

    /**
     * Get the period of time after which the first speculative read last add confirmed and entry
     * should be triggered.
     * A speculative entry request is sent to the next replica bookie before
     * an error or response has been received for the previous entry read request.
     *
     * <p>A speculative entry read is only sent if we have not heard from the current
     * replica bookie during the entire read operation which may comprise of many entries.
     *
     * <p>Speculative requests allow the client to avoid having to wait for the connect timeout
     * in the case that a bookie has failed. It induces higher load on the network and on
     * bookies. This should be taken into account before changing this configuration value.
     *
     * @return the speculative request timeout in milliseconds. Default 1500.
     */
    public int getFirstSpeculativeReadLACTimeout() {
        return getInt(FIRST_SPECULATIVE_READ_LAC_TIMEOUT, 1500);
    }


    /**
     * Get the maximum interval between successive speculative read last add confirmed and entry
     * requests.
     *
     * @return the max speculative request timeout in milliseconds. Default 5000.
     */
    public int getMaxSpeculativeReadLACTimeout() {
        return getInt(MAX_SPECULATIVE_READ_LAC_TIMEOUT, 5000);
    }

    /**
     * Set the period of time after which the first speculative read last add confirmed and entry
     * should be triggered.
     * A lower timeout will reduce read latency in the case of a failed bookie,
     * while increasing the load on bookies and the network.
     *
     * <p>The default is 1500 milliseconds. A value of 0 will disable speculative reads
     * completely.
     *
     * @see #getSpeculativeReadTimeout()
     * @param timeout the timeout value, in milliseconds
     * @return client configuration
     */
    public ClientConfiguration setFirstSpeculativeReadLACTimeout(int timeout) {
        setProperty(FIRST_SPECULATIVE_READ_LAC_TIMEOUT, timeout);
        return this;
    }

    /**
     * Set the maximum interval between successive speculative read last add confirmed and entry
     * requests.
     *
     * @param timeout the timeout value, in milliseconds
     * @return client configuration
     */
    public ClientConfiguration setMaxSpeculativeReadLACTimeout(int timeout) {
        setProperty(MAX_SPECULATIVE_READ_LAC_TIMEOUT, timeout);
        return this;
    }

    /**
     * Whether to enable parallel reading in recovery read.
     *
     * @return true if enable parallel reading in recovery read. otherwise, return false.
     */
    public boolean getEnableParallelRecoveryRead() {
        return getBoolean(ENABLE_PARALLEL_RECOVERY_READ, false);
    }

    /**
     * Enable/Disable parallel reading in recovery read.
     *
     * @param enabled
     *          flag to enable/disable parallel reading in recovery read.
     * @return client configuration.
     */
    public ClientConfiguration setEnableParallelRecoveryRead(boolean enabled) {
        setProperty(ENABLE_PARALLEL_RECOVERY_READ, enabled);
        return this;
    }

    /**
     * Get Recovery Read Batch Size.
     *
     * @return recovery read batch size.
     */
    public int getRecoveryReadBatchSize() {
        return getInt(RECOVERY_READ_BATCH_SIZE, 1);
    }

    /**
     * Set Recovery Read Batch Size.
     *
     * @param batchSize
     *          recovery read batch size.
     * @return client configuration.
     */
    public ClientConfiguration setRecoveryReadBatchSize(int batchSize) {
        setProperty(RECOVERY_READ_BATCH_SIZE, batchSize);
        return this;
    }

    /**
     * If reorder read sequence enabled or not.
     *
     * @return true if reorder read sequence is enabled, otherwise false.
     */
    public boolean isReorderReadSequenceEnabled() {
        return getBoolean(REORDER_READ_SEQUENCE_ENABLED, false);
    }

    /**
     * Enable/disable reordering read sequence on reading entries.
     *
     * <p>If this flag is enabled, the client will use
     * {@link EnsemblePlacementPolicy#reorderReadSequence(java.util.List,
     * org.apache.bookkeeper.client.BookiesHealthInfo, org.apache.bookkeeper.client.DistributionSchedule.WriteSet)}
     * to figure out a better read sequence to attempt reads from replicas and use
     * {@link EnsemblePlacementPolicy#reorderReadLACSequence(java.util.List,
     * org.apache.bookkeeper.client.BookiesHealthInfo, org.apache.bookkeeper.client.DistributionSchedule.WriteSet)}
     * to figure out a better read sequence to attempt long poll reads from replicas.
     *
     * <p>The order of read sequence is determined by the placement policy implementations.
     *
     * @param enabled the flag to enable/disable reorder read sequence.
     * @return client configuration instance.
     */
    public ClientConfiguration setReorderReadSequenceEnabled(boolean enabled) {
        setProperty(REORDER_READ_SEQUENCE_ENABLED, enabled);
        return this;
    }

    /**
     * If read operation should be sticky to a single bookie or not.
     *
     * @return true if reorder read sequence is enabled, otherwise false.
     */
    public boolean isStickyReadsEnabled() {
        return getBoolean(STICKY_READS_ENABLED, false);
    }

    /**
     * Enable/disable having read operations for a ledger to be sticky to
     * a single bookie.
     *
     * <p>If this flag is enabled, the client will use one single bookie (by
     * preference) to read all entries for a ledger.
     *
     * <p>Having all the read to one bookie will increase the chances that
     * a read request will be fullfilled by Bookie read cache (or OS file
     * system cache) when doing sequential reads.
     *
     * @param enabled the flag to enable/disable sticky reads.
     * @return client configuration instance.
     */
    public ClientConfiguration setStickyReadsEnabled(boolean enabled) {
        setProperty(STICKY_READS_ENABLED, enabled);
        return this;
    }

    /**
     * Get Ensemble Placement Policy Class.
     *
     * @return ensemble placement policy class.
     */
    public Class<? extends EnsemblePlacementPolicy> getEnsemblePlacementPolicy()
            throws ConfigurationException {
        return ReflectionUtils.getClass(this, ENSEMBLE_PLACEMENT_POLICY,
                RackawareEnsemblePlacementPolicy.class,
                EnsemblePlacementPolicy.class,
                                        DEFAULT_LOADER);
    }

    /**
     * Set Ensemble Placement Policy Class.
     *
     * @param policyClass
     *          Ensemble Placement Policy Class.
     */
    public ClientConfiguration setEnsemblePlacementPolicy(Class<? extends EnsemblePlacementPolicy> policyClass) {
        setProperty(ENSEMBLE_PLACEMENT_POLICY, policyClass.getName());
        return this;
    }

    /**
     * Get the threshold for the number of pending requests beyond which to reorder
     * reads. If &lt;= zero, this feature is turned off.
     *
     * @return the threshold for the number of pending requests beyond which to
     *         reorder reads.
     */
    public int getReorderThresholdPendingRequests() {
        return getInt(READ_REORDER_THRESHOLD_PENDING_REQUESTS, 0);
    }

    /**
     * Set the threshold for the number of pending requests beyond which to reorder
     * reads. If zero, this feature is turned off.
     *
     * @param threshold
     *            The threshold for the number of pending requests beyond which to
     *            reorder reads.
     */
    public ClientConfiguration setReorderThresholdPendingRequests(int threshold) {
        setProperty(READ_REORDER_THRESHOLD_PENDING_REQUESTS, threshold);
        return this;
    }

    /**
     * Get the network topology stabilize period in seconds. if it is zero, this feature is turned off.
     *
     * @return network topology stabilize period in seconds.
     */
    public int getNetworkTopologyStabilizePeriodSeconds() {
        return getInt(NETWORK_TOPOLOGY_STABILIZE_PERIOD_SECONDS, 0);
    }

    /**
     * Set the network topology stabilize period in seconds.
     *
     * @see #getNetworkTopologyStabilizePeriodSeconds()
     * @param seconds stabilize period in seconds
     * @return client configuration.
     */
    public ClientConfiguration setNetworkTopologyStabilizePeriodSeconds(int seconds) {
        setProperty(NETWORK_TOPOLOGY_STABILIZE_PERIOD_SECONDS, seconds);
        return this;
    }

    /**
     * Whether to order slow bookies in placement policy.
     *
     * @return flag of whether to order slow bookies in placement policy or not.
     */
    public boolean getEnsemblePlacementPolicySlowBookies() {
        return getBoolean(ENSEMBLE_PLACEMENT_POLICY_ORDER_SLOW_BOOKIES, false);
    }

    /**
     * Enable/Disable ordering slow bookies in placement policy.
     *
     * @param enabled
     *          flag to enable/disable ordering slow bookies in placement policy.
     * @return client configuration.
     */
    public ClientConfiguration setEnsemblePlacementPolicySlowBookies(boolean enabled) {
        setProperty(ENSEMBLE_PLACEMENT_POLICY_ORDER_SLOW_BOOKIES, enabled);
        return this;
    }

    /**
     * Whether to enable BookieAddressResolver.
     *
     * @return flag to enable/disable BookieAddressResolver.
     */
    public boolean getBookieAddressResolverEnabled() {
        return getBoolean(BOOKIE_ADDRESS_RESOLVER_ENABLED, true);
    }

    /**
     * Enable/Disable BookieAddressResolver.
     *
     * <p>
     * If this flag is true, read bookie information from the metadata service (e.g. ZooKeeper) to resolve the address
     * from each bookie ID. If all bookie IDs in the cluster are "address:port" or "hostname:port", you can set this
     * flag to false to reduce requests to the metadata service.
     * </p>
     *
     * @param enabled
     *          flag to enable/disable BookieAddressResolver.
     * @return client configuration.
     */
    public ClientConfiguration setBookieAddressResolverEnabled(boolean enabled) {
        setProperty(BOOKIE_ADDRESS_RESOLVER_ENABLED, enabled);
        return this;
    }

    /**
     * Set the flag to use hostname to resolve local node placement policy.
     * @param useHostnameResolveLocalNodePlacementPolicy
     */
    public void setUseHostnameResolveLocalNodePlacementPolicy(boolean useHostnameResolveLocalNodePlacementPolicy) {
        setProperty(USE_HOSTNAME_RESOLVE_LOCAL_NODE_PLACEMENT_POLICY, useHostnameResolveLocalNodePlacementPolicy);
    }

    /**
     * Get whether to use hostname to resolve local node placement policy.
     * @return
     */
    public boolean getUseHostnameResolveLocalNodePlacementPolicy() {
        return getBoolean(USE_HOSTNAME_RESOLVE_LOCAL_NODE_PLACEMENT_POLICY, false);
    }

    /**
     * Whether to enable recording task execution stats.
     *
     * @return flag to enable/disable recording task execution stats.
     */
    public boolean getEnableTaskExecutionStats() {
        return getBoolean(ENABLE_TASK_EXECUTION_STATS, false);
    }

    /**
     * Enable/Disable recording task execution stats.
     *
     * @param enabled
     *          flag to enable/disable recording task execution stats.
     * @return client configuration.
     */
    public ClientConfiguration setEnableTaskExecutionStats(boolean enabled) {
        setProperty(ENABLE_TASK_EXECUTION_STATS, enabled);
        return this;
    }

    /**
     * Get task execution duration which triggers a warning.
     *
     * @return time in microseconds which triggers a warning.
     */
    public long getTaskExecutionWarnTimeMicros() {
        return getLong(TASK_EXECUTION_WARN_TIME_MICROS, TimeUnit.SECONDS.toMicros(1));
    }

    /**
     * Set task execution duration which triggers a warning.
     *
     * @param warnTime
     *          time in microseconds which triggers a warning.
     * @return client configuration.
     */
    public ClientConfiguration setTaskExecutionWarnTimeMicros(long warnTime) {
        setProperty(TASK_EXECUTION_WARN_TIME_MICROS, warnTime);
        return this;
    }

    /**
     * Check if bookie health check is enabled.
     *
     * @return
     */
    public boolean isBookieHealthCheckEnabled() {
        return getBoolean(BOOKIE_HEALTH_CHECK_ENABLED, false);
    }

    /**
     * Enables the bookie health check.
     *
     * <p>
     * If the number of read/write errors for a bookie exceeds {@link #getBookieErrorThresholdPerInterval()} per
     * interval, that bookie is quarantined for {@link #getBookieQuarantineTimeSeconds()} seconds. During this
     * quarantined period, the client will try not to use this bookie when creating new ensembles.
     * </p>
     *
     * <p>By default, the bookie health check is <b>disabled</b>.
     *
     * @return client configuration
     */
    public ClientConfiguration enableBookieHealthCheck() {
        setProperty(BOOKIE_HEALTH_CHECK_ENABLED, true);
        return this;
    }

    /**
     * Get the bookie health check interval in seconds.
     *
     * @return
     */
    public int getBookieHealthCheckIntervalSeconds() {
        return getInt(BOOKIE_HEALTH_CHECK_INTERVAL_SECONDS, 60);
    }

    /**
     * Set the bookie health check interval. Default is 60 seconds.
     *
     * <p>
     * Note: Please {@link #enableBookieHealthCheck()} to use this configuration.
     * </p>
     *
     * @param interval
     * @param unit
     * @return client configuration
     */
    public ClientConfiguration setBookieHealthCheckInterval(int interval, TimeUnit unit) {
        setProperty(BOOKIE_HEALTH_CHECK_INTERVAL_SECONDS, unit.toSeconds(interval));
        return this;
    }

    /**
     * Get the error threshold for a bookie to be quarantined.
     *
     * @return
     */
    public long getBookieErrorThresholdPerInterval() {
        return getLong(BOOKIE_ERROR_THRESHOLD_PER_INTERVAL, 100);
    }

    /**
     * Set the error threshold per interval ({@link #getBookieHealthCheckIntervalSeconds()}) for a bookie before it is
     * quarantined. Default is 100 errors per minute.
     *
     * <p>
     * Note: Please {@link #enableBookieHealthCheck()} to use this configuration.
     * </p>
     *
     * @param thresholdPerInterval
     *
     * @return client configuration
     */
    public ClientConfiguration setBookieErrorThresholdPerInterval(long thresholdPerInterval) {
        setProperty(BOOKIE_ERROR_THRESHOLD_PER_INTERVAL, thresholdPerInterval);
        return this;
    }

    /**
     * Get the time for which a bookie will be quarantined.
     *
     * @return
     */
    public int getBookieQuarantineTimeSeconds() {
        return getInt(BOOKIE_QUARANTINE_TIME_SECONDS, 1800);
    }

    /**
     * Set the time for which a bookie will be quarantined. Default is 30 minutes.
     *
     * <p>
     * Note: Please {@link #enableBookieHealthCheck()} to use this configuration.
     * </p>
     *
     * @param quarantineTime
     * @param unit
     * @return client configuration
     */
    public ClientConfiguration setBookieQuarantineTime(int quarantineTime, TimeUnit unit) {
        setProperty(BOOKIE_QUARANTINE_TIME_SECONDS, unit.toSeconds(quarantineTime));
        return this;
    }

    /**
     * Get the bookie quarantine ratio.
     *
     * @return
     */
    public double getBookieQuarantineRatio() {
        return getDouble(BOOKIE_QUARANTINE_RATIO, 1.0);
    }

    /**
     * set the bookie quarantine ratio. default is 1.0.
     *
     * @param ratio
     * @return client configuration
     */
    public ClientConfiguration setBookieQuarantineRatio(double ratio) {
        setProperty(BOOKIE_QUARANTINE_RATIO, ratio);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClientConfiguration setNettyMaxFrameSizeBytes(int maxSize) {
        super.setNettyMaxFrameSizeBytes(maxSize);
        return this;
    }

    /**
     * Get the time interval between successive calls for bookie get info. Default is 24 hours.
     *
     * @return
     */
    public int getGetBookieInfoIntervalSeconds() {
        return getInt(GET_BOOKIE_INFO_INTERVAL_SECONDS, 24 * 60 * 60);
    }

    /**
     * Get the time interval between retries on unsuccessful bookie info request.  Default is
     * 60s.
     *
     * @return
     */
    public int getGetBookieInfoRetryIntervalSeconds() {
        return getInt(GET_BOOKIE_INFO_RETRY_INTERVAL_SECONDS, 60);
    }

    /**
     * Return whether disk weight based placement policy is enabled.
     * @return
     */
    public boolean getDiskWeightBasedPlacementEnabled() {
        return getBoolean(DISK_WEIGHT_BASED_PLACEMENT_ENABLED, false);
    }

    /**
     * Returns the max multiple to use for nodes with very high weight.
     * @return max multiple
     */
    public int getBookieMaxWeightMultipleForWeightBasedPlacement() {
        return getInt(BOOKIE_MAX_MULTIPLE_FOR_WEIGHTED_PLACEMENT, 3);
    }

    /**
     * Return the timeout value for getBookieInfo request.
     * @return
     */
    public int getBookieInfoTimeout() {
        return getInteger(GET_BOOKIE_INFO_TIMEOUT_SECS, 5);
    }

    /**
     * Return the timeout value for startTLS request.
     * @return
     */
    public int getStartTLSTimeout() {
        return getInteger(START_TLS_TIMEOUT_SECS, 10);
    }

    /**
     * Set whether or not disk weight based placement is enabled.
     *
     * @param isEnabled - boolean indicating enabled or not
     * @return client configuration
     */
    public ClientConfiguration setDiskWeightBasedPlacementEnabled(boolean isEnabled) {
        setProperty(DISK_WEIGHT_BASED_PLACEMENT_ENABLED, isEnabled);
        return this;
    }

    /**
     * Set the time interval between successive polls for bookie get info.
     *
     * @param pollInterval
     * @param unit
     * @return client configuration
     */
    public ClientConfiguration setGetBookieInfoIntervalSeconds(int pollInterval, TimeUnit unit) {
        setProperty(GET_BOOKIE_INFO_INTERVAL_SECONDS, unit.toSeconds(pollInterval));
        return this;
    }

    /**
     * Set the time interval between retries on unsuccessful GetInfo requests.
     *
     * @param interval
     * @param unit
     * @return client configuration
     */
    public ClientConfiguration setGetBookieInfoRetryIntervalSeconds(int interval, TimeUnit unit) {
        setProperty(GET_BOOKIE_INFO_RETRY_INTERVAL_SECONDS, unit.toSeconds(interval));
        return this;
    }

    /**
     * Set the max multiple to use for nodes with very high weight.
     *
     * @param multiple
     * @return client configuration
     */
    public ClientConfiguration setBookieMaxWeightMultipleForWeightBasedPlacement(int multiple) {
        setProperty(BOOKIE_MAX_MULTIPLE_FOR_WEIGHTED_PLACEMENT, multiple);
        return this;
    }

    /**
     * Set the timeout value in secs for the GET_BOOKIE_INFO request.
     *
     * @param timeoutSecs
     * @return client configuration
     */
    public ClientConfiguration setGetBookieInfoTimeout(int timeoutSecs) {
        setProperty(GET_BOOKIE_INFO_TIMEOUT_SECS, timeoutSecs);
        return this;
    }

    /**
     * Set the timeout value in secs for the START_TLS request.
     * @param timeoutSecs
     * @return client configuration
     */
    public ClientConfiguration setStartTLSTimeout(int timeoutSecs) {
        setProperty(START_TLS_TIMEOUT_SECS, timeoutSecs);
        return this;
    }

    /**
     * Whether hostname verification enabled?
     *
     * @return true if hostname verification enabled, otherwise false.
     */
    public boolean getHostnameVerificationEnabled() {
        return getBoolean(TLS_HOSTNAME_VERIFICATION_ENABLED, false);
    }

    /**
     * Enable/Disable hostname verification for tls connection.
     *
     * @param enabled
     *            flag to enable/disable tls hostname verification.
     * @return client configuration.
     */
    public ClientConfiguration setHostnameVerificationEnabled(boolean enabled) {
        setProperty(TLS_HOSTNAME_VERIFICATION_ENABLED, enabled);
        return this;
    }

    /**
     * Set the client role.
     *
     * @param role defines how the client will act
     * @return client configuration
     */
    public ClientConfiguration setClientRole(String role) {
        if (role == null) {
            throw new NullPointerException();
        }
        switch (role) {
            case CLIENT_ROLE_STANDARD:
            case CLIENT_ROLE_SYSTEM:
                break;
            default:
                throw new IllegalArgumentException("invalid role " + role);
        }
        setProperty(CLIENT_ROLE, role);
        return this;
    }

    /**
     * Get the role of the client.
     *
     * @return the type of client
     */
    public String getClientRole() {
        return getString(CLIENT_ROLE, CLIENT_ROLE_STANDARD);
    }

    /**
     * Get the keystore type for client. Default is JKS.
     *
     * @return
     */
    public String getTLSKeyStoreType() {
        return getString(CLIENT_TLS_KEYSTORE_TYPE, getString(TLS_KEYSTORE_TYPE, "JKS"));
    }


    /**
     * Set the keystore type for client.
     *
     * @return
     */
    public ClientConfiguration setTLSKeyStoreType(String arg) {
        setProperty(TLS_KEYSTORE_TYPE, arg);
        return this;
    }

    /**
     * Get the keystore path for the client.
     *
     * @return
     */
    public String getTLSKeyStore() {
        return getString(CLIENT_TLS_KEYSTORE, getString(TLS_KEYSTORE, null));
    }

    /**
     * Set the keystore path for the client.
     *
     * @return
     */
    public ClientConfiguration setTLSKeyStore(String arg) {
        setProperty(TLS_KEYSTORE, arg);
        return this;
    }

    /**
     * Get the path to file containing keystore password, if the client keystore is password protected. Default is null.
     *
     * @return
     */
    public String getTLSKeyStorePasswordPath() {
        return getString(CLIENT_TLS_KEYSTORE_PASSWORD_PATH, getString(TLS_KEYSTORE_PASSWORD_PATH, null));
    }

    /**
     * Set the path to file containing keystore password, if the client keystore is password protected.
     *
     * @return
     */
    public ClientConfiguration setTLSKeyStorePasswordPath(String arg) {
        setProperty(TLS_KEYSTORE_PASSWORD_PATH, arg);
        return this;
    }

    /**
     * Get the truststore type for client. Default is JKS.
     *
     * @return
     */
    public String getTLSTrustStoreType() {
        return getString(CLIENT_TLS_TRUSTSTORE_TYPE, getString(TLS_TRUSTSTORE_TYPE, "JKS"));
    }

    /**
     * Set the truststore type for client.
     *
     * @return
     */
    public ClientConfiguration setTLSTrustStoreType(String arg) {
        setProperty(TLS_TRUSTSTORE_TYPE, arg);
        return this;
    }

    /**
     * Get the truststore path for the client.
     *
     * @return
     */
    public String getTLSTrustStore() {
        return getString(CLIENT_TLS_TRUSTSTORE, getString(TLS_TRUSTSTORE, null));
    }

    /**
     * Set the truststore path for the client.
     *
     * @return
     */
    public ClientConfiguration setTLSTrustStore(String arg) {
        setProperty(TLS_TRUSTSTORE, arg);
        return this;
    }

    /**
     * Get the path to file containing truststore password, if the client truststore is password protected. Default is
     * null.
     *
     * @return
     */
    public String getTLSTrustStorePasswordPath() {
        return getString(CLIENT_TLS_TRUSTSTORE_PASSWORD_PATH, getString(TLS_TRUSTSTORE_PASSWORD_PATH, null));
    }

    /**
     * Set the path to file containing truststore password, if the client truststore is password protected.
     *
     * @return
     */
    public ClientConfiguration setTLSTrustStorePasswordPath(String arg) {
        setProperty(TLS_TRUSTSTORE_PASSWORD_PATH, arg);
        return this;
    }

    /**
     * Get the path to file containing TLS Certificate.
     *
     * @return
     */
    public String getTLSCertificatePath() {
        return getString(TLS_CERTIFICATE_PATH, null);
    }

    /**
     * Set the path to file containing TLS Certificate.
     *
     * @return
     */
    public ClientConfiguration setTLSCertificatePath(String arg) {
        setProperty(TLS_CERTIFICATE_PATH, arg);
        return this;
    }

    /**
     * Whether to allow opportunistic striping.
     *
     * @return true if opportunistic striping is enabled
     */
    public boolean getOpportunisticStriping() {
        return getBoolean(OPPORTUNISTIC_STRIPING, false);
    }

    /**
     * Enable/Disable opportunistic striping.
     * <p>
     * If set to true, when you are creating a ledger with a given
     * ensemble size, the system will automatically handle the
     * lack of enough bookies, reducing ensemble size up to
     * the write quorum size. This way in little clusters
     * you can try to use striping (ensemble size > write quorum size)
     * in case that you have enough bookies up and running,
     * and degrade automatically to the minimum requested replication count.
     * </p>
     *
     * @param enabled
     *          flag to enable/disable opportunistic striping.
     * @return client configuration.
     */
    public ClientConfiguration setOpportunisticStriping(boolean enabled) {
        setProperty(OPPORTUNISTIC_STRIPING, enabled);
        return this;
    }

    /**
     * Whether to delay ensemble change or not?
     *
     * @return true if to delay ensemble change, otherwise false.
     */
    public boolean getDelayEnsembleChange() {
        return getBoolean(DELAY_ENSEMBLE_CHANGE, false);
    }

    /**
     * Enable/Disable delaying ensemble change.
     * <p>
     * If set to true, ensemble change only happens when it can't meet
     * ack quorum requirement. If set to false, ensemble change happens
     * immediately when it received a failed write.
     * </p>
     *
     * @param enabled
     *          flag to enable/disable delaying ensemble change.
     * @return client configuration.
     */
    public ClientConfiguration setDelayEnsembleChange(boolean enabled) {
        setProperty(DELAY_ENSEMBLE_CHANGE, enabled);
        return this;
    }

    /**
     * Whether to enable bookie address changes tracking.
     *
     * @return flag to enable/disable bookie address changes tracking
     */
    public boolean getEnableBookieAddressTracking() {
        return getBoolean(FOLLOW_BOOKIE_ADDRESS_TRACKING, true);
    }

    /**
     * Enable/Disable bookie address changes tracking.
     *
     * @param value
     *          flag to enable/disable bookie address changes tracking
     * @return client configuration.
     */
    public ClientConfiguration setEnableBookieAddressTracking(boolean value) {
        setProperty(FOLLOW_BOOKIE_ADDRESS_TRACKING, value);
        return this;
    }

    /**
     * Whether to enable bookie failure tracking.
     *
     * @return flag to enable/disable bookie failure tracking
     */
    public boolean getEnableBookieFailureTracking() {
        return getBoolean(ENABLE_BOOKIE_FAILURE_TRACKING, true);
    }

    /**
     * Enable/Disable bookie failure tracking.
     *
     * @param enabled
     *          flag to enable/disable bookie failure tracking
     * @return client configuration.
     */
    public ClientConfiguration setEnableBookieFailureTracking(boolean enabled) {
        setProperty(ENABLE_BOOKIE_FAILURE_TRACKING, enabled);
        return this;
    }

    /**
     * Get the bookie failure tracking expiration timeout.
     *
     * @return bookie failure tracking expiration timeout.
     */
    public int getBookieFailureHistoryExpirationMSec() {
        return getInt(BOOKIE_FAILURE_HISTORY_EXPIRATION_MS, 60000);
    }

    /**
     * Set the bookie failure tracking expiration timeout.
     *
     * @param expirationMSec
     *          bookie failure tracking expiration timeout.
     * @return client configuration.
     */
    public ClientConfiguration setBookieFailureHistoryExpirationMSec(int expirationMSec) {
        setProperty(BOOKIE_FAILURE_HISTORY_EXPIRATION_MS, expirationMSec);
        return this;
    }

    /**
     * Get the name of the dynamic feature that disables ensemble change.
     *
     * @return name of the dynamic feature that disables ensemble change
     */
    public String getDisableEnsembleChangeFeatureName() {
        return getString(DISABLE_ENSEMBLE_CHANGE_FEATURE_NAME, FEATURE_DISABLE_ENSEMBLE_CHANGE);
    }

    /**
     * Set the name of the dynamic feature that disables ensemble change.
     *
     * @param disableEnsembleChangeFeatureName
     *          name of the dynamic feature that disables ensemble change
     * @return client configuration.
     */
    public ClientConfiguration setDisableEnsembleChangeFeatureName(String disableEnsembleChangeFeatureName) {
        setProperty(DISABLE_ENSEMBLE_CHANGE_FEATURE_NAME, disableEnsembleChangeFeatureName);
        return this;
    }

    /**
     * Get the max allowed ensemble change number.
     *
     * @return value of MaxAllowedEnsembleChanges, default MAX_VALUE, indicating feature is disable.
     */
    public int getMaxAllowedEnsembleChanges() {
        return getInt(MAX_ALLOWED_ENSEMBLE_CHANGES, Integer.MAX_VALUE);
    }

    /**
     * Set the max allowed ensemble change number.
     *
     * @param num
     *          value of MaxAllowedEnsembleChanges
     * @return client configuration.
     */
    public ClientConfiguration setMaxAllowedEnsembleChanges(int num) {
        setProperty(MAX_ALLOWED_ENSEMBLE_CHANGES, num);
        return this;
    }

    /**
     * Option to use Netty Pooled ByteBufs.
     *
     * @deprecated see {@link BookKeeperBuilder#allocator(io.netty.buffer.ByteBufAllocator)}
     *
     * @return the value of the option
     */
    @Deprecated
    public boolean isNettyUsePooledBuffers() {
        return getBoolean(NETTY_USE_POOLED_BUFFERS, true);
    }

    /**
     * Enable/Disable the usage of Pooled Netty buffers. While using v2 wire protocol the application will be
     * responsible for releasing ByteBufs returned by BookKeeper.
     *
     * @param enabled
     *          if enabled BookKeeper will use default Pooled Netty Buffer allocator
     *
     * @deprecated see {@link BookKeeperBuilder#allocator(io.netty.buffer.ByteBufAllocator)}
     *
     * @see #setUseV2WireProtocol(boolean)
     * @see ByteBuf#release()
     * @see LedgerHandle#readEntries(long, long)
     */
    public ClientConfiguration setNettyUsePooledBuffers(boolean enabled) {
        setProperty(NETTY_USE_POOLED_BUFFERS, enabled);
        return this;
    }

    /**
     * Set registration manager class.
     *
     * @param regClientClass
     *            ClientClass
     * @deprecated since 4.7.0
     */
    @Deprecated
    public ClientConfiguration setRegistrationClientClass(
            Class<? extends RegistrationClient> regClientClass) {
        setProperty(REGISTRATION_CLIENT_CLASS, regClientClass);
        return this;
    }

    /**
     * Get Registration Client Class.
     *
     * @return registration manager class.
     * @deprecated since 4.7.0
     */
    @Deprecated
    public Class<? extends RegistrationClient> getRegistrationClientClass()
            throws ConfigurationException {
        return ReflectionUtils.getClass(this, REGISTRATION_CLIENT_CLASS,
                ZKRegistrationClient.class, RegistrationClient.class,
                DEFAULT_LOADER);
    }

    /**
     * Enable the client to use system time as the ledger creation time.
     *
     * <p>If this is enabled, the client will write a ctime field into the ledger metadata.
     * Otherwise, nothing will be written. The creation time of this ledger will be the ctime
     * of the metadata record in metadata store.
     *
     * @param enabled flag to enable/disable client using system time as the ledger creation time.
     */
    public ClientConfiguration setStoreSystemtimeAsLedgerCreationTime(boolean enabled) {
        setProperty(STORE_SYSTEMTIME_AS_LEDGER_CREATION_TIME, enabled);
        return this;
    }

    /**
     * Return the flag that indicates whether client is using system time as the ledger creation time when
     * creating ledgers.
     *
     * @return the flag that indicates whether client is using system time as the ledger creation time when
     *         creating ledgers.
     */
    public boolean getStoreSystemtimeAsLedgerCreationTime() {
        return getBoolean(STORE_SYSTEMTIME_AS_LEDGER_CREATION_TIME, false);
    }

    /**
     * Set the log frequency when a bookie is unavailable, in order to limit log filesize.
     *
     * @param throttleValue
     * @param unit
     * @return client configuration.
     */
    public ClientConfiguration setClientConnectBookieUnavailableLogThrottling(
            int throttleValue, TimeUnit unit) {
        setProperty(CLIENT_CONNECT_BOOKIE_UNAVAILABLE_LOG_THROTTLING, unit.toMillis(throttleValue));
        return this;
    }

    /**
     * Get the log frequency when a bookie is unavailable, in milliseconds.
     *
     * @return log frequency when a bookie is unavailable, in milliseconds.
     */
    public long getClientConnectBookieUnavailableLogThrottlingMs() {
        return getLong(CLIENT_CONNECT_BOOKIE_UNAVAILABLE_LOG_THROTTLING, 5_000L);
    }

    public boolean isBatchReadEnabled() {
        return getBoolean(BATCH_READ_ENABLED, true);
    }

    @Override
    protected ClientConfiguration getThis() {
        return this;
    }
}
