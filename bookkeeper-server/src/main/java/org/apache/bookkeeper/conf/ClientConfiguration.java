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

import static com.google.common.base.Charsets.UTF_8;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;

/**
 * Configuration settings for client side
 */
public class ClientConfiguration extends AbstractConfiguration {

    // Zookeeper Parameters
    protected final static String ZK_TIMEOUT = "zkTimeout";
    protected final static String ZK_SERVERS = "zkServers";

    // Throttle value
    protected final static String THROTTLE = "throttle";

    // Digest Type
    protected final static String DIGEST_TYPE = "digestType";
    // Passwd
    protected final static String PASSWD = "passwd";

    // NIO Parameters
    protected final static String CLIENT_TCP_NODELAY = "clientTcpNoDelay";
    protected final static String CLIENT_SENDBUFFER_SIZE = "clientSendBufferSize";
    protected final static String CLIENT_RECEIVEBUFFER_SIZE = "clientReceiveBufferSize";
    protected final static String CLIENT_WRITEBUFFER_LOW_WATER_MARK = "clientWriteBufferLowWaterMark";
    protected final static String CLIENT_WRITEBUFFER_HIGH_WATER_MARK = "clientWriteBufferHighWaterMark";
    protected final static String CLIENT_CONNECT_TIMEOUT_MILLIS = "clientConnectTimeoutMillis";
    protected final static String NUM_CHANNELS_PER_BOOKIE = "numChannelsPerBookie";
    // Read Parameters
    protected final static String READ_TIMEOUT = "readTimeout";
    protected final static String SPECULATIVE_READ_TIMEOUT = "speculativeReadTimeout";
    // Timeout Setting
    protected final static String ADD_ENTRY_TIMEOUT_SEC = "addEntryTimeoutSec";
    protected final static String ADD_ENTRY_QUORUM_TIMEOUT_SEC = "addEntryQuorumTimeoutSec";
    protected final static String READ_ENTRY_TIMEOUT_SEC = "readEntryTimeoutSec";
    protected final static String TIMEOUT_TASK_INTERVAL_MILLIS = "timeoutTaskIntervalMillis";
    protected final static String PCBC_TIMEOUT_TIMER_TICK_DURATION_MS = "pcbcTimeoutTimerTickDurationMs";
    protected final static String PCBC_TIMEOUT_TIMER_NUM_TICKS = "pcbcTimeoutTimerNumTicks";

    // Number Woker Threads
    protected final static String NUM_WORKER_THREADS = "numWorkerThreads";

    // Ensemble Placement Policy
    protected final static String ENSEMBLE_PLACEMENT_POLICY = "ensemblePlacementPolicy";

    // Stats
    protected final static String ENABLE_TASK_EXECUTION_STATS = "enableTaskExecutionStats";
    protected final static String TASK_EXECUTION_WARN_TIME_MICROS = "taskExecutionWarnTimeMicros";

    /**
     * Construct a default client-side configuration
     */
    public ClientConfiguration() {
        super();
    }

    /**
     * Construct a client-side configuration using a base configuration
     *
     * @param conf
     *          Base configuration
     */
    public ClientConfiguration(AbstractConfiguration conf) {
        super();
        loadConf(conf);
    }

    /**
     * Get throttle value
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
     * Since BookKeeper process requests in asynchrous way, it will holds
     * those pending request in queue. You may easily run it out of memory
     * if producing too many requests than the capability of bookie servers can handle.
     * To prevent that from happeding, you can set a throttle value here.
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
     * Get digest type used in bookkeeper admin
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
     * Digest Type and Passwd used to open ledgers for admin tool
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
     * Get passwd used in bookkeeper admin
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
     * Digest Type and Passwd used to open ledgers for admin tool
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
     * This settings is used to enabled/disabled Nagle's algorithm, which is a means of
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
        return getInt(CLIENT_WRITEBUFFER_LOW_WATER_MARK, 32 * 1024);
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
        return getInt(CLIENT_WRITEBUFFER_HIGH_WATER_MARK, 64 * 1024);
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
     * Get zookeeper servers to connect
     *
     * @return zookeeper servers
     */
    public String getZkServers() {
        List servers = getList(ZK_SERVERS, null);
        if (null == servers || 0 == servers.size()) {
            return "localhost";
        }
        return StringUtils.join(servers, ",");
    }

    /**
     * Set zookeeper servers to connect
     *
     * @param zkServers
     *          ZooKeeper servers to connect
     */
    public ClientConfiguration setZkServers(String zkServers) {
        setProperty(ZK_SERVERS, zkServers);
        return this;
    }

    /**
     * Get zookeeper timeout
     *
     * @return zookeeper client timeout
     */
    public int getZkTimeout() {
        return getInt(ZK_TIMEOUT, 10000);
    }

    /**
     * Set zookeeper timeout
     *
     * @param zkTimeout
     *          ZooKeeper client timeout
     * @return client configuration
     */
    public ClientConfiguration setZkTimeout(int zkTimeout) {
        setProperty(ZK_TIMEOUT, Integer.toString(zkTimeout));
        return this;
    }

    /**
     * Get the socket read timeout. This is the number of
     * seconds we wait without hearing a response from a bookie
     * before we consider it failed.
     *
     * The default is 5 seconds.
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
     * The default value is 5 second for backwards compatibility.
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
     * Get the interval between successive executions of the PerChannelBookieClient's
     * TimeoutTask. This value is in milliseconds. Every X milliseconds, the timeout task
     * will be executed and it will error out entries that have timed out.
     *
     * We do it more aggressive to not accumulate pending requests due to slow responses.
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
     * Get the tick duration in milliseconds that used for the
     * {@link org.jboss.netty.util.HashedWheelTimer} that used by PCBC to timeout
     * requests.
     *
     * @see org.jboss.netty.util.HashedWheelTimer
     *
     * @return tick duration in milliseconds
     */
    public long getPCBCTimeoutTimerTickDurationMs() {
        return getLong(PCBC_TIMEOUT_TIMER_TICK_DURATION_MS, 100);
    }

    /**
     * Set the tick duration in milliseconds that used for
     * {@link org.jboss.netty.util.HashedWheelTimer} that used by PCBC to timeout
     * requests. Be aware of {@link org.jboss.netty.util.HashedWheelTimer} if you
     * are going to modify this setting.
     *
     * @see #getPCBCTimeoutTimerTickDurationMs()
     *
     * @param tickDuration
     *          tick duration in milliseconds.
     * @return client configuration.
     */
    public ClientConfiguration setPCBCTimeoutTimerTickDurationMs(long tickDuration) {
        setProperty(PCBC_TIMEOUT_TIMER_TICK_DURATION_MS, tickDuration);
        return this;
    }

    /**
     * Get number of ticks that used for
     * {@link org.jboss.netty.util.HashedWheelTimer} that used by PCBC to timeout
     * requests.
     *
     * @see org.jboss.netty.util.HashedWheelTimer
     *
     * @return number of ticks that used for timeout timer.
     */
    public int getPCBCTimeoutTimerNumTicks() {
        return getInt(PCBC_TIMEOUT_TIMER_NUM_TICKS, 1024);
    }

    /**
     * Set number of ticks that used for
     * {@link org.jboss.netty.util.HashedWheelTimer} that used by PCBC to timeout request.
     * Be aware of {@link org.jboss.netty.util.HashedWheelTimer} if you are going to modify
     * this setting.
     *
     * @see #getPCBCTimeoutTimerNumTicks()
     *
     * @param numTicks
     *          number of ticks that used for timeout timer.
     * @return client configuration.
     */
    public ClientConfiguration setPCBCTimeoutTimerNumTicks(int numTicks) {
        setProperty(PCBC_TIMEOUT_TIMER_NUM_TICKS, numTicks);
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
     * Get the period of time after which a speculative entry read should be triggered.
     * A speculative entry read is sent to the next replica bookie before
     * an error or response has been received for the previous entry read request.
     *
     * A speculative entry read is only sent if we have not heard from the current
     * replica bookie during the entire read operation which may comprise of many entries.
     *
     * Speculative reads allow the client to avoid having to wait for the connect timeout
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
     * The default is 2000 milliseconds. A value of 0 will disable speculative reads
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
     * Get Ensemble Placement Policy Class.
     *
     * @return ensemble placement policy class.
     */
    public Class<? extends EnsemblePlacementPolicy> getEnsemblePlacementPolicy()
        throws ConfigurationException {
        return ReflectionUtils.getClass(this, ENSEMBLE_PLACEMENT_POLICY,
                                        RackawareEnsemblePlacementPolicy.class,
                                        EnsemblePlacementPolicy.class,
                                        defaultLoader);
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
}
