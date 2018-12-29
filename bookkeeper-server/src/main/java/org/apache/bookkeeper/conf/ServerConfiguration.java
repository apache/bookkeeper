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

import static org.apache.bookkeeper.util.BookKeeperConstants.MAX_LOG_SIZE_LIMIT;

import com.google.common.annotations.Beta;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.conf.ConfigDef;
import org.apache.bookkeeper.common.conf.ConfigException;
import org.apache.bookkeeper.common.conf.ConfigKey;
import org.apache.bookkeeper.common.conf.ConfigKeyGroup;
import org.apache.bookkeeper.common.conf.Type;
import org.apache.bookkeeper.common.conf.Validator;
import org.apache.bookkeeper.common.conf.validators.ClassValidator;
import org.apache.bookkeeper.common.conf.validators.RangeValidator;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.ConfigurationException;

/**
 * Configuration manages server-side settings.
 */
public class ServerConfiguration extends AbstractConfiguration<ServerConfiguration> {

    //
    // Security Settings
    //

    protected static final String BOOKIE_AUTH_PROVIDER_FACTORY_CLASS = "bookieAuthProviderFactoryClass";
    protected static final ConfigKey BOOKIE_AUTH_PROVIDER_FACTORY_CLASS_KEY =
        ConfigKey.builder(BOOKIE_AUTH_PROVIDER_FACTORY_CLASS)
            .type(Type.CLASS)
            .description("The bookie authentication provider factory class name")
            .documentation("If this is null, no authentication will take place")
            .group(GROUP_SECURITY)
            .orderInGroup(100)
            .build();

    public static final String PERMITTED_STARTUP_USERS = "permittedStartupUsers";
    protected static final ConfigKey PERMITTED_STARTUP_USERS_KEY = ConfigKey.builder(PERMITTED_STARTUP_USERS)
        .type(Type.ARRAY)
        .description("The list of users are permitted to run the bookie process. any users can run the bookie"
            + " process if it is not set.")
        .documentation("Example settings: permittedStartupUsers=user1,user2,user3")
        .group(GROUP_SECURITY)
        .orderInGroup(101)
        .build();

    //
    // Metadata Settings
    //

    protected static final String REGISTRATION_MANAGER_CLASS = "registrationManagerClass";
    protected static final ConfigKey REGISTRATION_MANAGER_CLASS_KEY = ConfigKey.builder(REGISTRATION_MANAGER_CLASS)
        .type(Type.CLASS)
        .description("The registration manager implementation used for registering bookies for service discovery")
        .defaultValue(ZKRegistrationManager.class)
        .group(GROUP_METADATA_SERVICE)
        .orderInGroup(100)
        .build();

    //
    // Discovery Settings
    //

    private static final ConfigKeyGroup GROUP_BOOKIE_DISCOVERY = ConfigKeyGroup.builder("discovery")
        .description("Bookie discovery related settings (e.g. nic & port to listen on, advertised address, and etc)")
        .order(100)
        .build();

    protected static final String BOOKIE_PORT = "bookiePort";
    protected static final ConfigKey BOOKIE_PORT_KEY = ConfigKey.builder(BOOKIE_PORT)
        .type(Type.INT)
        .description("The port that the bookie server listens on")
        .defaultValue(3181)
        .validator(RangeValidator.atLeast(0))
        .group(GROUP_BOOKIE_DISCOVERY)
        .orderInGroup(100)
        .build();

    protected static final String LISTENING_INTERFACE = "listeningInterface";
    protected static final ConfigKey LISTENING_INTERFACE_KEY = ConfigKey.builder(LISTENING_INTERFACE)
        .type(Type.STRING)
        .description("Set the network interface that the bookie should listen on")
        .documentation("If not set, the bookie will listen on all interfaces")
        .group(GROUP_BOOKIE_DISCOVERY)
        .orderInGroup(101)
        .build();

    protected static final String ADVERTISED_ADDRESS = "advertisedAddress";
    protected static final ConfigKey ADVERTISED_ADDRESS_KEY = ConfigKey.builder(ADVERTISED_ADDRESS)
        .type(Type.STRING)
        .description("Configure a specific hostname or IP address that the bookie should use to"
            + " advertise itself to clients")
        .group(GROUP_BOOKIE_DISCOVERY)
        .orderInGroup(102)
        .build();

    protected static final String ALLOW_LOOPBACK = "allowLoopback";
    protected static final ConfigKey ALLOW_LOOPBACK_KEY = ConfigKey.builder(ALLOW_LOOPBACK)
        .type(Type.BOOLEAN)
        .description("Whether the bookie allowed to use a loopback interface as its primary"
            + " interface(i.e. the interface it uses to establish its identity)?")
        .documentation("By default, loopback interfaces are not allowed as the primary interface."
            + " Using a loopback interface as the primary interface usually indicates a configuration"
            + " error. For example, its fairly common in some VPS setups to not configure a hostname,"
            + " or to have the hostname resolve to 127.0.0.1. If this is the case, then all bookies in"
            + " the cluster will establish their identities as 127.0.0.1:3181, and only one will be able"
            + " to join the cluster. For VPSs configured like this, you should explicitly set the listening"
            + " interface.")
        .defaultValue(false)
        .group(GROUP_BOOKIE_DISCOVERY)
        .orderInGroup(103)
        .build();

    protected static final String ALLOW_EPHEMERAL_PORTS = "allowEphemeralPorts";
    protected static final ConfigKey ALLOW_EPHEMERAL_PORTS_KEY = ConfigKey.builder(ALLOW_EPHEMERAL_PORTS)
        .type(Type.BOOLEAN)
        .description("Whether the bookie is allowed to use an ephemeral port (port 0) as its server port")
        .documentation("By default, an ephemeral port is not allowed. Using an ephemeral port as the service"
            + " port usually indicates a configuration error. However, in unit tests, using an ephemeral"
            + " port will address port conflict problems and allow running tests in parallel")
        .defaultValue(false)
        .group(GROUP_BOOKIE_DISCOVERY)
        .orderInGroup(104)
        .build();


    protected static final String USE_HOST_NAME_AS_BOOKIE_ID = "useHostNameAsBookieID";
    protected static final String USE_SHORT_HOST_NAME = "useShortHostName";

    protected static final ConfigKey USE_HOST_NAME_AS_BOOKIE_ID_KEY = ConfigKey.builder(USE_HOST_NAME_AS_BOOKIE_ID)
        .type(Type.BOOLEAN)
        .description("Whether the bookie should use its hostname to register with the co-ordination service"
            + " (eg: Zookeeper service)")
        .documentation("When false, bookie will use its ipaddress of '" + LISTENING_INTERFACE
            + "' for the registration")
        .defaultValue(false)
        .dependents(Lists.newArrayList(USE_SHORT_HOST_NAME))
        .group(GROUP_BOOKIE_DISCOVERY)
        .orderInGroup(105)
        .build();

    protected static final ConfigKey USE_SHORT_HOST_NAME_KEY = ConfigKey.builder(USE_SHORT_HOST_NAME)
        .type(Type.BOOLEAN)
        .description("If bookie is using hostname for registration and in ledger metadata then whether"
            + " to use short hostname or FQDN hostname")
        .defaultValue(false)
        .group(GROUP_BOOKIE_DISCOVERY)
        .orderInGroup(106)
        .build();

    protected static final String ENABLE_LOCAL_TRANSPORT = "enableLocalTransport";
    protected static final ConfigKey ENABLE_LOCAL_TRANSPORT_KEY = ConfigKey.builder(ENABLE_LOCAL_TRANSPORT)
        .type(Type.BOOLEAN)
        .description("Whether allow the bookie to listen for BookKeeper clients executed on the local JVM")
        .defaultValue(false)
        .group(GROUP_BOOKIE_DISCOVERY)
        .orderInGroup(107)
        .build();

    protected static final String DISABLE_SERVER_SOCKET_BIND = "disableServerSocketBind";
    protected static final ConfigKey DISABLE_SERVER_SOCKET_BIND_KEY = ConfigKey.builder(DISABLE_SERVER_SOCKET_BIND)
        .type(Type.BOOLEAN)
        .description("Whether allow the bookie to disable bind on network interfaces, this bookie"
            + " will be available only to BookKeeper clients executed on the local JVM")
        .defaultValue(false)
        .group(GROUP_BOOKIE_DISCOVERY)
        .orderInGroup(108)
        .build();

    //
    // Server Settings
    //

    private static final ConfigKeyGroup GROUP_SERVER = ConfigKeyGroup.builder("server")
        .description("Generic bookie server settings")
        .order(110)
        .build();

    protected static final String ALLOW_MULTIPLEDIRS_UNDER_SAME_DISKPARTITION =
        "allowMultipleDirsUnderSameDiskPartition";
    protected static final ConfigKey ALLOW_MULTIPLEDIRS_UNDER_SAME_DISKPARTITION_KEY =
        ConfigKey.builder(ALLOW_MULTIPLEDIRS_UNDER_SAME_DISKPARTITION)
            .type(Type.BOOLEAN)
            .description("Configure the bookie to allow/disallow multiple ledger/index/journal directories"
                + " in the same filesystem disk partition")
            .defaultValue(true)
            .group(GROUP_SERVER)
            .orderInGroup(100)
            .build();

    protected static final String DEATH_WATCH_INTERVAL = "bookieDeathWatchInterval";
    protected static final ConfigKey DEATH_WATCH_INTERVAL_KEY = ConfigKey.builder(DEATH_WATCH_INTERVAL)
        .type(Type.INT)
        .description("Interval to watch whether bookie is dead or not, in milliseconds")
        .defaultValue(1000)
        .group(GROUP_SERVER)
        .orderInGroup(101)
        .build();

    // Lifecycle Components
    protected static final String EXTRA_SERVER_COMPONENTS = "extraServerComponents";
    protected static final ConfigKey EXTRA_SERVER_COMPONENTS_KEY = ConfigKey.builder(EXTRA_SERVER_COMPONENTS)
        .type(Type.ARRAY)
        .description("Configure a list of server components to enable and load on a bookie server")
        .documentation("This provides the plugin run extra services along with a bookie server.\n\n"
            + "NOTE: if bookie fails to load any of extra components configured below, bookie will continue"
            + "  functioning by ignoring the components configured below.")
        .optionValues(Lists.newArrayList(
            "org.apache.bookkeeper.stream.server.StreamStorageLifecycleComponent"
        ))
        .group(GROUP_SERVER)
        .orderInGroup(102)
        .build();

    protected static final String IGNORE_EXTRA_SERVER_COMPONENTS_STARTUP_FAILURES =
        "ignoreExtraServerComponentsStartupFailures";
    protected static final ConfigKey IGNORE_EXTRA_SERVER_COMPONENTS_STARTUP_FAILURES_KEY =
        ConfigKey.builder(IGNORE_EXTRA_SERVER_COMPONENTS_STARTUP_FAILURES)
            .type(Type.BOOLEAN)
            .description("Whether the bookie should ignore startup failures on loading server components specified"
                + " by `" + EXTRA_SERVER_COMPONENTS + "`")
            .defaultValue(false)
            .group(GROUP_SERVER)
            .orderInGroup(103)
            .build();

    //
    // Thread settings
    //

    private static final ConfigKeyGroup GROUP_THREAD = ConfigKeyGroup.builder("thread")
        .description("Thread related settings")
        .order(120)
        .build();

    protected static final String NUM_ADD_WORKER_THREADS = "numAddWorkerThreads";
    protected static final ConfigKey NUM_ADD_WORKER_THREADS_KEY = ConfigKey.builder(NUM_ADD_WORKER_THREADS)
        .type(Type.INT)
        .description("Number of threads that should handle write requests")
        .documentation("If zero, the writes would be handled by netty threads directly")
        .defaultValue(1)
        .group(GROUP_THREAD)
        .orderInGroup(100)
        .build();

    protected static final String NUM_READ_WORKER_THREADS = "numReadWorkerThreads";
    protected static final ConfigKey NUM_READ_WORKER_THREADS_KEY = ConfigKey.builder(NUM_READ_WORKER_THREADS)
        .type(Type.INT)
        .description("Number of threads that should handle read requests")
        .documentation("If zero, the reads would be handled by netty threads directly")
        .defaultValue(8)
        .group(GROUP_THREAD)
        .orderInGroup(101)
        .build();

    protected static final String NUM_LONG_POLL_WORKER_THREADS = "numLongPollWorkerThreads";
    protected static final ConfigKey NUM_LONG_POLL_WORKER_THREADS_KEY = ConfigKey.builder(NUM_LONG_POLL_WORKER_THREADS)
        .type(Type.INT)
        .description("Number of threads that should handle read requests")
        .documentation("If zero, the reads would be handled by netty threads directly")
        .defaultValue(0)
        .group(GROUP_THREAD)
        .orderInGroup(102)
        .build();

    protected static final String NUM_JOURNAL_CALLBACK_THREADS = "numJournalCallbackThreads";
    protected static final ConfigKey NUM_JOURNAL_CALLBACK_THREADS_KEY = ConfigKey.builder(NUM_JOURNAL_CALLBACK_THREADS)
        .type(Type.INT)
        .description("The number of threads used for handling journal callback")
        .documentation("If a zero or negative number is provided, the callbacks are executed"
            + " directly at force write threads")
        .defaultValue(1)
        .group(GROUP_THREAD)
        .orderInGroup(103)
        .build();

    protected static final String NUM_HIGH_PRIORITY_WORKER_THREADS = "numHighPriorityWorkerThreads";
    protected static final ConfigKey NUM_HIGH_PRIORITY_WORKER_THREADS_KEY =
        ConfigKey.builder(NUM_HIGH_PRIORITY_WORKER_THREADS)
            .type(Type.INT)
            .description("Number of threads that should be used for high priority requests"
                + " (i.e. recovery reads and adds, and fencing)")
            .defaultValue(8)
            .group(GROUP_THREAD)
            .orderInGroup(104)
            .build();

    protected static final String MAX_PENDING_READ_REQUESTS_PER_THREAD = "maxPendingReadRequestsPerThread";
    protected static final ConfigKey MAX_PENDING_READ_REQUESTS_PER_THREAD_KEY =
        ConfigKey.builder(MAX_PENDING_READ_REQUESTS_PER_THREAD)
            .type(Type.INT)
            .description("If read workers threads are enabled, limit the number of pending requests,"
                + " to avoid the executor queue to grow indefinitely")
            .defaultValue(10000)
            .group(GROUP_THREAD)
            .orderInGroup(105)
            .build();

    protected static final String MAX_PENDING_ADD_REQUESTS_PER_THREAD = "maxPendingAddRequestsPerThread";
    protected static final ConfigKey MAX_PENDING_ADD_REQUESTS_PER_THREAD_KEY =
        ConfigKey.builder(MAX_PENDING_ADD_REQUESTS_PER_THREAD)
            .type(Type.INT)
            .description("If add workers threads are enabled, limit the number of pending requests,"
                + " to avoid the executor queue to grow indefinitely")
            .defaultValue(10000)
            .group(GROUP_THREAD)
            .orderInGroup(106)
            .build();

    //
    // LongPoll Settings
    //

    private static final ConfigKeyGroup GROUP_LONGPOLL = ConfigKeyGroup.builder("longpoll")
        .description("Long poll related settings")
        .order(130)
        .build();

    protected static final String REQUEST_TIMER_TICK_DURATION_MILLISEC = "requestTimerTickDurationMs";
    protected static final ConfigKey REQUEST_TIMER_TICK_DURATION_MILLISEC_KEY =
        ConfigKey.builder(REQUEST_TIMER_TICK_DURATION_MILLISEC)
            .type(Type.INT)
            .description("The tick duration in milliseconds for long poll requests")
            .defaultValue(10)
            .group(GROUP_LONGPOLL)
            .orderInGroup(100)
            .build();

    protected static final String REQUEST_TIMER_NO_OF_TICKS = "requestTimerNumTicks";
    protected static final ConfigKey REQUEST_TIMER_NO_OF_TICKS_KEY =
        ConfigKey.builder(REQUEST_TIMER_NO_OF_TICKS)
            .type(Type.INT)
            .description("The number of ticks per wheel for the long poll request timer")
            .defaultValue(1024)
            .group(GROUP_LONGPOLL)
            .orderInGroup(101)
            .build();

    //
    // Backpressure control settings
    //

    private static final ConfigKeyGroup GROUP_BACKPRESSURE = ConfigKeyGroup.builder("backpressure")
        .description("Backpressure control related settings")
        .order(140)
        .build();

    protected static final String MAX_ADDS_IN_PROGRESS_LIMIT = "maxAddsInProgressLimit";
    protected static final ConfigKey MAX_ADDS_IN_PROGRESS_LIMIT_KEY =
        ConfigKey.builder(MAX_ADDS_IN_PROGRESS_LIMIT)
            .type(Type.INT)
            .description("max number of adds in progress. 0 == unlimited")
            .documentation("If the number of add requests in progress reaches this threshold, bookie"
                + " will be blocking the add threads util some add requests are completed")
            .defaultValue(0)
            .group(GROUP_BACKPRESSURE)
            .orderInGroup(100)
            .build();

    protected static final String MAX_READS_IN_PROGRESS_LIMIT = "maxReadsInProgressLimit";
    protected static final ConfigKey MAX_READS_IN_PROGRESS_LIMIT_KEY =
        ConfigKey.builder(MAX_READS_IN_PROGRESS_LIMIT)
            .type(Type.INT)
            .description("max number of adds in progress. 0 == unlimited")
            .documentation("If the number of read requests in progress reaches this threshold, bookie"
                + " will be blocking the read threads util some read requests are completed")
            .defaultValue(0)
            .group(GROUP_BACKPRESSURE)
            .orderInGroup(101)
            .build();

    protected static final String WAIT_TIMEOUT_ON_RESPONSE_BACKPRESSURE = "waitTimeoutOnResponseBackpressureMs";
    protected static final ConfigKey WAIT_TIMEOUT_ON_RESPONSE_BACKPRESSURE_KEY =
        ConfigKey.builder(WAIT_TIMEOUT_ON_RESPONSE_BACKPRESSURE)
            .type(Type.LONG)
            .description("Timeout controlling wait on response send in case of unresponsive client"
                + " (i.e. client in long GC etc.)")
            .documentation("negative value disables the feature, 0 to allow request to fail immediately")
            .defaultValue(-1L)
            .group(GROUP_BACKPRESSURE)
            .orderInGroup(102)
            .build();

    protected static final String CLOSE_CHANNEL_ON_RESPONSE_TIMEOUT = "closeChannelOnResponseTimeout";
    protected static final ConfigKey CLOSE_CHANNEL_ON_RESPONSE_TIMEOUT_KEY =
        ConfigKey.builder(CLOSE_CHANNEL_ON_RESPONSE_TIMEOUT)
            .type(Type.BOOLEAN)
            .documentation("Configures action in case if server timed out sending response to the client."
                + " `true` == close the channel and drop response; `false` == drop response. It requires"
                + " `" + WAIT_TIMEOUT_ON_RESPONSE_BACKPRESSURE + "` >= 0 otherwise ignored.")
            .defaultValue(false)
            .group(GROUP_BACKPRESSURE)
            .orderInGroup(103)
            .build();

    //
    // Read-only mode support
    //

    private static final ConfigKeyGroup GROUP_READONLY = ConfigKeyGroup.builder("readonly")
        .description("Read-only mode related settings")
        .order(150)
        .build();

    protected static final String READ_ONLY_MODE_ENABLED = "readOnlyModeEnabled";
    protected static final ConfigKey READ_ONLY_MODE_ENABLED_KEY = ConfigKey.builder(READ_ONLY_MODE_ENABLED)
        .type(Type.BOOLEAN)
        .description("If all ledger directories configured are full, then support only read requests for clients")
        .documentation("If `" + READ_ONLY_MODE_ENABLED + "=true` then on all ledger disks full,"
            + " bookie will be converted to read-only mode and serve only read requests."
            + " Otherwise the bookie will be shutdown. By default this will be disabled.")
        .defaultValue(true)
        .group(GROUP_READONLY)
        .orderInGroup(100)
        .build();


    protected static final String FORCE_READ_ONLY_BOOKIE = "forceReadOnlyBookie";
    protected static final ConfigKey FORCE_READ_ONLY_BOOKIE_KEY = ConfigKey.builder(FORCE_READ_ONLY_BOOKIE)
        .type(Type.BOOLEAN)
        .description("Whether the bookie is force started in read only mode or not")
        .defaultValue(false)
        .group(GROUP_READONLY)
        .orderInGroup(101)
        .build();

    //Whether to persist the bookie status
    protected static final String PERSIST_BOOKIE_STATUS_ENABLED = "persistBookieStatusEnabled";
    protected static final ConfigKey PERSIST_BOOKIE_STATUS_ENABLED_KEY =
        ConfigKey.builder(PERSIST_BOOKIE_STATUS_ENABLED)
            .type(Type.BOOLEAN)
            .description("Persist the bookie status locally on the disks. So the bookies can keep their"
                + " status upon restarts")
            .defaultValue(false)
            .group(GROUP_READONLY)
            .orderInGroup(102)
            .since("4.6")
            .build();

    //
    // Netty Server Settings
    //

    protected static final String SERVER_TCP_NODELAY = "serverTcpNoDelay";
    protected static final ConfigKey SERVER_TCP_NODELAY_KEY = ConfigKey.builder(SERVER_TCP_NODELAY)
        .type(Type.BOOLEAN)
        .description("This settings is used to enabled/disabled Nagle's algorithm")
        .documentation("The Nagle's algorithm is a means of improving the efficiency of"
            + " TCP/IP networks by reducing the number of packets that need to be sent"
            + " over the network. If you are sending many small messages, such that more"
            + " than one can fit in a single IP packet, setting server.tcpnodelay to false"
            + " to enable Nagle algorithm can provide better performance.")
        .defaultValue(true)
        .group(GROUP_NETTY)
        .orderInGroup(100)
        .build();

    protected static final String SERVER_SOCK_KEEPALIVE = "serverSockKeepalive";
    protected static final ConfigKey SERVER_SOCK_KEEPALIVE_KEY = ConfigKey.builder(SERVER_SOCK_KEEPALIVE)
        .type(Type.BOOLEAN)
        .description("This setting is used to send keep-alive messages on connection-oriented sockets")
        .defaultValue(true)
        .group(GROUP_NETTY)
        .orderInGroup(101)
        .build();

    protected static final String SERVER_SOCK_LINGER = "serverTcpLinger";
    protected static final ConfigKey SERVER_SOCK_LINGER_KEY = ConfigKey.builder(SERVER_SOCK_LINGER)
        .type(Type.INT)
        .description("The socket linger timeout on close")
        .documentation("When enabled, a close or shutdown will not return until all queued messages for"
            + " the socket have been successfully sent or the linger timeout has been reached."
            + " Otherwise, the call returns immediately and the closing is done in the background")
        .defaultValue(0)
        .group(GROUP_NETTY)
        .orderInGroup(102)
        .build();

    protected static final String SERVER_WRITEBUFFER_LOW_WATER_MARK = "serverWriteBufferLowWaterMark";
    protected static final ConfigKey SERVER_WRITEBUFFER_LOW_WATER_MARK_KEY =
        ConfigKey.builder(SERVER_WRITEBUFFER_LOW_WATER_MARK)
            .type(Type.INT)
            .description("server netty channel write buffer low water mark")
            .documentation("Once the number of bytes queued in the write buffer exceeded the `"
                + SERVER_WRITEBUFFER_LOW_WATER_MARK + "` and then dropped down below this value,"
                + " a netty channel `Channel.isWritable()` will start to return true again.")
            .defaultValue(384 * 1024)
            .group(GROUP_NETTY)
            .orderInGroup(103)
            .build();

    protected static final String SERVER_WRITEBUFFER_HIGH_WATER_MARK = "serverWriteBufferHighWaterMark";
    protected static final ConfigKey SERVER_WRITEBUFFER_HIGH_WATER_MARK_KEY =
        ConfigKey.builder(SERVER_WRITEBUFFER_HIGH_WATER_MARK)
            .type(Type.INT)
            .description("server netty channel write buffer high water mark")
            .documentation("If the number of bytes queued in the write buffer exceeds this value,"
                + " a netty channel `Channel.isWritable()` will start to return false.")
            .defaultValue(512 * 1024)
            .group(GROUP_NETTY)
            .orderInGroup(104)
            .build();

    protected static final String SERVER_NUM_IO_THREADS = "serverNumIOThreads";
    protected static final ConfigKey SERVER_NUM_IO_THREADS_KEY = ConfigKey.builder(SERVER_NUM_IO_THREADS)
        .type(Type.INT)
        .description("The number of netty IO threads")
        .documentation("This is the number of threads used by Netty to handle TCP connections")
        .defaultValueSupplier(conf -> 2 * Runtime.getRuntime().availableProcessors())
        .group(GROUP_NETTY)
        .orderInGroup(105)
        .build();

    protected static final String BYTEBUF_ALLOCATOR_SIZE_INITIAL = "byteBufAllocatorSizeInitial";
    protected static final ConfigKey BYTEBUF_ALLOCATOR_SIZE_INITIAL_KEY =
        ConfigKey.builder(BYTEBUF_ALLOCATOR_SIZE_INITIAL)
            .type(Type.INT)
            .description("The Recv ByteBuf allocator initial buf size")
            .defaultValue(66536)
            .group(GROUP_NETTY)
            .orderInGroup(106)
            .build();

    protected static final String BYTEBUF_ALLOCATOR_SIZE_MIN = "byteBufAllocatorSizeMin";
    protected static final ConfigKey BYTEBUF_ALLOCATOR_SIZE_MIN_KEY =
        ConfigKey.builder(BYTEBUF_ALLOCATOR_SIZE_MIN)
            .type(Type.INT)
            .description("The Recv ByteBuf allocator min buf size")
            .defaultValue(66536)
            .group(GROUP_NETTY)
            .orderInGroup(107)
            .build();

    protected static final String BYTEBUF_ALLOCATOR_SIZE_MAX = "byteBufAllocatorSizeMax";
    protected static final ConfigKey BYTEBUF_ALLOCATOR_SIZE_MAX_KEY =
        ConfigKey.builder(BYTEBUF_ALLOCATOR_SIZE_MAX)
            .type(Type.INT)
            .description("The Recv ByteBuf allocator max buf size")
            .defaultValue(1048576)
            .group(GROUP_NETTY)
            .orderInGroup(108)
            .build();

    //
    // Http Server Settings
    //

    private static final ConfigKeyGroup GROUP_HTTP = ConfigKeyGroup.builder("http")
        .description("Admin http server related settings")
        .order(160)
        .build();

    protected static final String HTTP_SERVER_ENABLED = "httpServerEnabled";
    protected static final ConfigKey HTTP_SERVER_ENABLED_KEY = ConfigKey.builder(HTTP_SERVER_ENABLED)
        .type(Type.BOOLEAN)
        .description("The flag enables/disables starting the admin http server")
        .defaultValue(false)
        .group(GROUP_HTTP)
        .orderInGroup(100)
        .build();

    protected static final String HTTP_SERVER_PORT = "httpServerPort";
    protected static final ConfigKey HTTP_SERVER_PORT_KEY = ConfigKey.builder(HTTP_SERVER_PORT)
        .type(Type.INT)
        .description("The http server port to listen on")
        .defaultValue(8080)
        .group(GROUP_HTTP)
        .orderInGroup(101)
        .build();

    protected static final String HTTP_SERVER_CLASS = "httpServerClass";
    protected static final ConfigKey HTTP_SERVER_CLASS_KEY = ConfigKey.builder(HTTP_SERVER_CLASS)
        .type(Type.CLASS)
        .description("The http server class")
        .defaultValue("org.apache.bookkeeper.http.vertx.VertxHttpServer")
        .group(GROUP_HTTP)
        .orderInGroup(102)
        .build();

    //
    // Journal Settings
    //

    private static final ConfigKeyGroup GROUP_JOURNAL = ConfigKeyGroup.builder("journal")
        .description("Journal related settings")
        .order(170)
        .build();

    protected static final String JOURNAL_DIRS = "journalDirectories";
    protected static final ConfigKey JOURNAL_DIRS_KEY = ConfigKey.builder(JOURNAL_DIRS)
        .type(Type.ARRAY)
        .description("Directories BookKeeper outputs its write ahead log")
        .documentation("You could define multi directories to store write head logs, separated by ','."
            + " For example: `journalDirectories=/tmp/bk-journal1,/tmp/bk-journal2`. If journalDirectories"
            + " is set, bookies will skip journalDirectory and use this setting directory.")
        .defaultValue("/tmp/bk-txn")
        .group(GROUP_JOURNAL)
        .orderInGroup(100)
        .deprecated(true)
        .deprecatedSince("4.5.0")
        .deprecatedByConfigKey(JOURNAL_DIRS)
        .build();

    protected static final String JOURNAL_DIR = "journalDirectory";
    protected static final ConfigKey JOURNAL_DIR_KEY = ConfigKey.builder(JOURNAL_DIR)
        .type(Type.STRING)
        .description("Directory Bookkeeper outputs its write ahead log")
        .defaultValue("/tmp/bk-txn")
        .group(GROUP_JOURNAL)
        .orderInGroup(100)
        .deprecated(true)
        .deprecatedSince("4.5.0")
        .deprecatedByConfigKey(JOURNAL_DIRS)
        .build();

    protected static final String JOURNAL_ALIGNMENT_SIZE = "journalAlignmentSize";

    protected static final String JOURNAL_FORMAT_VERSION_TO_WRITE = "journalFormatVersionToWrite";
    protected static final ConfigKey JOURNAL_FORMAT_VERSION_TO_WRITE_KEY =
        ConfigKey.builder(JOURNAL_FORMAT_VERSION_TO_WRITE)
            .type(Type.INT)
            .description("The journal format version to write")
            .documentation("If you'd like to disable persisting ExplicitLac, you can set this"
                + " config to < `6` and also # fileInfoFormatVersionToWrite should be set"
                + " to 0. If there is mismatch then the serverconfig is considered invalid."
                + " You can disable `padding-writes` by setting journal version back to `4`."
                + " This feature is available since 4.5.0 and onward versions.")
            .optionValues(Lists.newArrayList(
                " 1: no header",
                " 2: a header section was added",
                " 3: ledger key was introduced",
                " 4: fencing key was introduced",
                " 5: expanding header to 512 and padding writes to align sector size configured"
                    + " by `" + JOURNAL_ALIGNMENT_SIZE + "`",
                " 6: persisting explicitLac is introduced"
            ))
            .defaultValue(6)
            .group(GROUP_JOURNAL)
            .orderInGroup(101)
            .build();

    protected static final String MAX_JOURNAL_SIZE = "journalMaxSizeMB";
    protected static final ConfigKey MAX_JOURNAL_SIZE_KEY = ConfigKey.builder(MAX_JOURNAL_SIZE)
        .type(Type.LONG)
        .description("Max file size of journal file, in mega bytes")
        .documentation("A new journal file will be created when the old one reaches the file size limitation")
        .defaultValue(2048L)
        .group(GROUP_JOURNAL)
        .orderInGroup(102)
        .build();

    protected static final String MAX_BACKUP_JOURNALS = "journalMaxBackups";
    protected static final ConfigKey MAX_BACKUP_JOURNALS_KEY = ConfigKey.builder(MAX_BACKUP_JOURNALS)
        .type(Type.INT)
        .description("Max number of old journal file to kept")
        .documentation("Keep a number of old journal files would help data recovery in special case")
        .defaultValue(5)
        .group(GROUP_JOURNAL)
        .orderInGroup(103)
        .build();

    protected static final String JOURNAL_PRE_ALLOC_SIZE = "journalPreAllocSizeMB";
    protected static final ConfigKey JOURNAL_PRE_ALLOC_SIZE_KEY = ConfigKey.builder(JOURNAL_PRE_ALLOC_SIZE)
        .type(Type.INT)
        .description("How much space should we pre-allocate at a time in the journal")
        .defaultValue(16)
        .group(GROUP_JOURNAL)
        .orderInGroup(104)
        .build();

    protected static final String JOURNAL_WRITE_BUFFER_SIZE = "journalWriteBufferSizeKB";
    protected static final ConfigKey JOURNAL_WRITE_BUFFER_SIZE_KEY = ConfigKey.builder(JOURNAL_WRITE_BUFFER_SIZE)
        .type(Type.INT)
        .description("Size of the write buffers used for the journal")
        .defaultValue(64)
        .group(GROUP_JOURNAL)
        .orderInGroup(105)
        .build();

    protected static final String JOURNAL_REMOVE_FROM_PAGE_CACHE = "journalRemoveFromPageCache";
    protected static final ConfigKey JOURNAL_REMOVE_FROM_PAGE_CACHE_KEY =
        ConfigKey.builder(JOURNAL_REMOVE_FROM_PAGE_CACHE)
            .type(Type.BOOLEAN)
            .description("Should we remove pages from page cache after force write")
            .defaultValue(true)
            .group(GROUP_JOURNAL)
            .orderInGroup(106)
            .build();

    protected static final String JOURNAL_SYNC_DATA = "journalSyncData";;
    protected static final ConfigKey JOURNAL_SYNC_DATA_KEY = ConfigKey.builder(JOURNAL_SYNC_DATA)
        .type(Type.BOOLEAN)
        .description("Should the data be fsynced on journal before acknowledgment")
        .documentation("By default, data sync is enabled to guarantee durability of writes."
            + " Beware: while disabling data sync in the Bookie journal might improve the"
            + " bookie write performance, it will also introduce the possibility of data"
            + " loss. With no sync, the journal entries are written in the OS page cache"
            + " but not flushed to disk. In case of power failure, the affected bookie might"
            + " lose the unflushed data. If the ledger is replicated to multiple bookies,"
            + " the chances of data loss are reduced though still present.")
        .defaultValue(true)
        .group(GROUP_JOURNAL)
        .orderInGroup(107)
        .build();

    protected static final String JOURNAL_ADAPTIVE_GROUP_WRITES = "journalAdaptiveGroupWrites";
    protected static final ConfigKey JOURNAL_ADAPTIVE_GROUP_WRITES_KEY =
        ConfigKey.builder(JOURNAL_ADAPTIVE_GROUP_WRITES)
            .type(Type.BOOLEAN)
            .description("Should we group journal force writes, which optimize group commit for higher throughput")
            .defaultValue(true)
            .group(GROUP_JOURNAL)
            .orderInGroup(108)
            .build();

    protected static final String JOURNAL_MAX_GROUP_WAIT_MSEC = "journalMaxGroupWaitMSec";
    protected static final ConfigKey JOURNAL_MAX_GROUP_WAIT_MSEC_KEY =
        ConfigKey.builder(JOURNAL_MAX_GROUP_WAIT_MSEC)
            .type(Type.LONG)
            .description("Maximum latency to impose on a journal write to achieve grouping")
            .defaultValue(2L)
            .group(GROUP_JOURNAL)
            .orderInGroup(109)
            .build();

    protected static final String JOURNAL_BUFFERED_WRITES_THRESHOLD = "journalBufferedWritesThreshold";
    protected static final ConfigKey JOURNAL_BUFFERED_WRITES_THRESHOLD_KEY =
        ConfigKey.builder(JOURNAL_BUFFERED_WRITES_THRESHOLD)
            .type(Type.LONG)
            .description("Maximum bytes to buffer to achieve grouping. 0 or negative value means disabling"
                + " bytes-based grouping")
            .defaultValue(524288L)
            .group(GROUP_JOURNAL)
            .orderInGroup(110)
            .build();

    protected static final String JOURNAL_BUFFERED_ENTRIES_THRESHOLD = "journalBufferedEntriesThreshold";
    protected static final ConfigKey JOURNAL_BUFFERED_ENTRIES_THRESHOLD_KEY =
        ConfigKey.builder(JOURNAL_BUFFERED_ENTRIES_THRESHOLD)
            .type(Type.LONG)
            .description("Maximum entries to buffer to impose on a journal write to achieve grouping."
                + " 0 or negative value means disable entries-based grouping")
            .defaultValue(0L)
            .group(GROUP_JOURNAL)
            .orderInGroup(111)
            .build();

    protected static final String JOURNAL_FLUSH_WHEN_QUEUE_EMPTY = "journalFlushWhenQueueEmpty";
    protected static final ConfigKey JOURNAL_FLUSH_WHEN_QUEUE_EMPTY_KEY =
        ConfigKey.builder(JOURNAL_FLUSH_WHEN_QUEUE_EMPTY)
            .type(Type.BOOLEAN)
            .description("If we should flush the journal when journal queue is empty")
            .defaultValue(false)
            .group(GROUP_JOURNAL)
            .orderInGroup(112)
            .build();

    protected static final ConfigKey JOURNAL_ALIGNMENT_SIZE_KEY = ConfigKey.builder(JOURNAL_ALIGNMENT_SIZE)
        .type(Type.INT)
        .description("All the journal writes and commits should be aligned to given size")
        .documentation("If not, zeros will be padded to align to given size. It only takes effects"
            + " when `" + JOURNAL_FORMAT_VERSION_TO_WRITE + "` is set to 5")
        .defaultValue(512)
        .validator(new Validator() {
            @Override
            public boolean validate(String name, Object value) {
                if (value instanceof Number) {
                    int size = ((Number) value).intValue();
                    return size >= 512 && (size % 512) == 0;
                } else {
                    return false;
                }
            }

            @Override
            public String toString() {
                return "(n * 512) bytes";
            }
        })
        .group(GROUP_JOURNAL)
        .orderInGroup(113)
        .build();

    //
    // Entry Logger Settings
    //

    private static final ConfigKeyGroup GROUP_LEDGER_STORAGE_ENTRY_LOGGER = ConfigKeyGroup.builder("entrylogger")
        .description("EntryLogger related settings")
        .order(180)
        .build();

    protected static final String ENTRY_LOG_SIZE_LIMIT = "logSizeLimit";
    protected static final ConfigKey ENTRY_LOG_SIZE_LIMIT_KEY = ConfigKey.builder(ENTRY_LOG_SIZE_LIMIT)
        .type(Type.LONG)
        .description("Max file size of entry logger, in bytes")
        .documentation("A new entry log file will be created when the old one reaches this file size limitation")
        .defaultValue(MAX_LOG_SIZE_LIMIT)
        .validator(RangeValidator.between(0, MAX_LOG_SIZE_LIMIT))
        .group(GROUP_LEDGER_STORAGE_ENTRY_LOGGER)
        .orderInGroup(0)
        .build();

    protected static final String ENTRY_LOG_FILE_PREALLOCATION_ENABLED = "entryLogFilePreallocationEnabled";
    protected static final ConfigKey ENTRY_LOG_FILE_PREALLOCATION_ENABLED_KEY =
        ConfigKey.builder(ENTRY_LOG_FILE_PREALLOCATION_ENABLED)
            .type(Type.BOOLEAN)
            .description("Enable/Disable entry logger preallocation")
            .defaultValue(true)
            .group(GROUP_LEDGER_STORAGE_ENTRY_LOGGER)
            .orderInGroup(1)
            .build();

    protected static final String FLUSH_ENTRYLOG_INTERVAL_BYTES = "flushEntrylogBytes";
    protected static final ConfigKey FLUSH_ENTRYLOG_INTERVAL_BYTES_KEY =
        ConfigKey.builder(FLUSH_ENTRYLOG_INTERVAL_BYTES)
            .type(Type.LONG)
            .description("Entry log flush interval in bytes")
            .documentation("Default is 0. 0 or less disables this feature and effectively flush"
                + " happens on log rotation.\nFlushing in smaller chunks but more frequently"
                + " reduces spikes in disk I/O. Flushing too frequently may also affect"
                + " performance negatively.")
            .defaultValue(0)
            .group(GROUP_LEDGER_STORAGE_ENTRY_LOGGER)
            .orderInGroup(2)
            .build();

    protected static final String READ_BUFFER_SIZE = "readBufferSizeBytes";
    protected static final ConfigKey READ_BUFFER_SIZE_KEY = ConfigKey.builder(READ_BUFFER_SIZE)
        .type(Type.INT)
        .description("The number of bytes we should use as capacity for BufferedReadChannel. Default is 512 bytes.")
        .defaultValue(512)
        .validator(RangeValidator.atLeast(0))
        .group(GROUP_LEDGER_STORAGE_ENTRY_LOGGER)
        .orderInGroup(3)
        .build();

    protected static final String WRITE_BUFFER_SIZE = "writeBufferSizeBytes";
    protected static final ConfigKey WRITE_BUFFER_SIZE_KEY = ConfigKey.builder(WRITE_BUFFER_SIZE)
        .type(Type.INT)
        .description("The number of bytes used as capacity for the write buffer. Default is 64KB.")
        .defaultValue(64 * 1024)
        .validator(RangeValidator.atLeast(0))
        .group(GROUP_LEDGER_STORAGE_ENTRY_LOGGER)
        .orderInGroup(4)
        .build();

    protected static final String ENTRY_LOG_PER_LEDGER_ENABLED = "entryLogPerLedgerEnabled";
    protected static final String NUMBER_OF_MEMTABLE_FLUSH_THREADS = "numOfMemtableFlushThreads";
    protected static final String ENTRYLOGMAP_ACCESS_EXPIRYTIME_INSECONDS = "entrylogMapAccessExpiryTimeInSeconds";
    protected static final String MAXIMUM_NUMBER_OF_ACTIVE_ENTRYLOGS = "maximumNumberOfActiveEntryLogs";
    protected static final String ENTRY_LOG_PER_LEDGER_COUNTER_LIMITS_MULT_FACTOR =
        "entryLogPerLedgerCounterLimitsMultFactor";

    protected static final ConfigKey ENTRY_LOG_PER_LEDGER_ENABLED_KEY = ConfigKey.builder(ENTRY_LOG_PER_LEDGER_ENABLED)
        .type(Type.BOOLEAN)
        .description("Specifies if entryLog per ledger is enabled/disabled")
        .documentation("If it is enabled, then there would be a active entrylog for each ledger."
            + " It would be ideal to enable this feature if the underlying storage device has"
            + " multiple DiskPartitions or SSD and if in a given moment, entries of fewer number"
            + " of active ledgers are written to a bookie.")
        .defaultValue(false)
        .group(GROUP_LEDGER_STORAGE_ENTRY_LOGGER)
        .orderInGroup(5)
        .dependents(Lists.newArrayList(
            NUMBER_OF_MEMTABLE_FLUSH_THREADS,
            ENTRYLOGMAP_ACCESS_EXPIRYTIME_INSECONDS,
            MAXIMUM_NUMBER_OF_ACTIVE_ENTRYLOGS
        ))
        .build();

    protected static final ConfigKey NUMBER_OF_MEMTABLE_FLUSH_THREADS_KEY =
        ConfigKey.builder(NUMBER_OF_MEMTABLE_FLUSH_THREADS)
            .type(Type.INT)
            .description("In the case of multipleentrylogs, multiple threads can be used to flush the memtable")
            .defaultValue(8)
            .validator(RangeValidator.atLeast(0))
            .group(GROUP_LEDGER_STORAGE_ENTRY_LOGGER)
            .orderInGroup(6)
            .build();

    protected static final ConfigKey ENTRYLOGMAP_ACCESS_EXPIRYTIME_INSECONDS_KEY =
        ConfigKey.builder(ENTRYLOGMAP_ACCESS_EXPIRYTIME_INSECONDS)
            .type(Type.INT)
            .description("The amount of time EntryLogManagerForEntryLogPerLedger should wait for closing"
                + " the entrylog file after the last addEntry call for that ledger, if explicit writeclose"
                + " for that ledger is not received")
            .defaultValue(300)
            .validator(RangeValidator.atLeast(0))
            .group(GROUP_LEDGER_STORAGE_ENTRY_LOGGER)
            .orderInGroup(7)
            .build();

    protected static final ConfigKey MAXIMUM_NUMBER_OF_ACTIVE_ENTRYLOGS_KEY =
        ConfigKey.builder(MAXIMUM_NUMBER_OF_ACTIVE_ENTRYLOGS)
            .type(Type.INT)
            .description("In entryLogPerLedger feature, this specifies the maximum number of entrylogs that"
                + " can be active at a given point in time")
            .documentation("If there are more number of active entryLogs then the `"
                + MAXIMUM_NUMBER_OF_ACTIVE_ENTRYLOGS + "` then the entrylog will be evicted from the cache.")
            .defaultValue(500)
            .validator(RangeValidator.atLeast(0))
            .group(GROUP_LEDGER_STORAGE_ENTRY_LOGGER)
            .orderInGroup(8)
            .build();

    protected static final ConfigKey ENTRY_LOG_PER_LEDGER_COUNTER_LIMITS_MULT_FACTOR_KEY =
        ConfigKey.builder(ENTRY_LOG_PER_LEDGER_COUNTER_LIMITS_MULT_FACTOR)
            .type(Type.INT)
            .description("In entryLogPerLedger feature, this config value specifies the metrics cache size"
                + " limits in multiples of entrylogMap cache size limits")
            .defaultValue(10)
            .validator(RangeValidator.atLeast(0))
            .group(GROUP_LEDGER_STORAGE_ENTRY_LOGGER)
            .orderInGroup(9)
            .build();

    //
    // Ledger Storage Settings
    //

    private static final ConfigKeyGroup GROUP_LEDGER_STORAGE = ConfigKeyGroup.builder("ledgerstorage")
        .description("Ledger Storage related settings")
        .order(190) // place a place holder here
        .build();

    protected static final String LEDGER_STORAGE_CLASS = "ledgerStorageClass";
    protected static final ConfigKey LEDGER_STORAGE_CLASS_KEY = ConfigKey.builder(LEDGER_STORAGE_CLASS)
        .type(Type.CLASS)
        .description("Ledger storage implementation class")
        .defaultValue(SortedLedgerStorage.class)
        .optionValues(Lists.newArrayList(
            InterleavedLedgerStorage.class.getName(),
            SortedLedgerStorage.class.getName(),
            DbLedgerStorage.class.getName()
        ))
        .validator(ClassValidator.of(LedgerStorage.class))
        .group(GROUP_LEDGER_STORAGE)
        .orderInGroup(0)
        .build();

    protected static final String SORTED_LEDGER_STORAGE_ENABLED = "sortedLedgerStorageEnabled";
    protected static final ConfigKey SORTED_LEDGER_STORAGE_ENABLED_KEY =
        ConfigKey.builder(SORTED_LEDGER_STORAGE_ENABLED)
            .type(Type.BOOLEAN)
            .description("Whether to use sorted ledger storage or not")
            .defaultValue(true)
            .group(GROUP_LEDGER_STORAGE)
            .orderInGroup(1)
            .deprecated(true)
            .deprecatedByConfigKey(LEDGER_STORAGE_CLASS)
            .build();

    protected static final String LEDGER_DIRS = "ledgerDirectories";
    protected static final ConfigKey LEDGER_DIRS_KEY = ConfigKey.builder(LEDGER_DIRS)
        .type(Type.ARRAY)
        .description("Directories that a bookie outputs ledger storage snapshots")
        .documentation(
            "Ideally ledger dirs and journal dirs are ach in a different device,"
                + " which reduce the contention between random I/O and sequential write."
                + " It is possible to run with a single disk, but performance will be"
                + " significantly lower.")
        .defaultValue(new String[] { "/tmp/bk-data" })
        .group(GROUP_LEDGER_STORAGE)
        .orderInGroup(2)
        .build();

    protected static final String INDEX_DIRS = "indexDirectories";
    protected static final ConfigKey INDEX_DIRS_KEY = ConfigKey.builder(INDEX_DIRS)
        .type(Type.ARRAY)
        .description("Directories that a bookie stores index files")
        .documentation("If not specified, `" + LEDGER_DIRS + "` will be used to store the index files.")
        .group(GROUP_LEDGER_STORAGE)
        .orderInGroup(3)
        .build();

    protected static final String MIN_USABLESIZE_FOR_INDEXFILE_CREATION = "minUsableSizeForIndexFileCreation";
    protected static final ConfigKey MIN_USABLESIZE_FOR_INDEXFILE_CREATION_KEY =
        ConfigKey.builder(MIN_USABLESIZE_FOR_INDEXFILE_CREATION)
            .type(Type.LONG)
            .description("Minimum safe usable size to be available in index directory for bookie to create"
                + " index file while replaying journal at the time of bookie start in readonly mode (in bytes)")
            .defaultValue(100 * 1024 * 1024L)
            .validator(RangeValidator.atLeast(0L))
            .group(GROUP_LEDGER_STORAGE)
            .orderInGroup(4)
            .build();

    protected static final String MIN_USABLESIZE_FOR_ENTRYLOG_CREATION = "minUsableSizeForEntryLogCreation";
    protected static final ConfigKey MIN_USABLESIZE_FOR_ENTRYLOG_CREATION_KEY =
        ConfigKey.builder(MIN_USABLESIZE_FOR_ENTRYLOG_CREATION)
            .type(Type.LONG)
            .description("Minimum safe usable size to be available in ledger directory for bookie to create"
                + " entry log files (in bytes)")
            .documentation("This parameter allows creating entry log files when there are enough disk spaces,"
                + " even when the bookie is running at readonly mode because of the disk usage is exceeding"
                + " `diskUsageThreshold`. Because compaction, journal replays can still write data to disks"
                + " when a bookie is readonly.\n\n"
                + "Default value is 1.2 * `" + ENTRY_LOG_SIZE_LIMIT + "`.")
            .validator(RangeValidator.atLeast(0L))
            .defaultValueSupplier(conf -> 1.2 * ENTRY_LOG_SIZE_LIMIT_KEY.getLong(conf))
            .group(GROUP_LEDGER_STORAGE)
            .orderInGroup(5)
            .build();

    protected static final String MIN_USABLESIZE_FOR_HIGH_PRIORITY_WRITES = "minUsableSizeForHighPriorityWrites";
    protected static final ConfigKey MIN_USABLESIZE_FOR_HIGH_PRIORITY_WRITES_KEY =
        ConfigKey.builder(MIN_USABLESIZE_FOR_HIGH_PRIORITY_WRITES)
            .type(Type.LONG)
            .description("Minimum safe usable size to be available in ledger directory for bookie to accept"
                + " high priority writes even it is in readonly mode.")
            .documentation("If not set, it is the value of `" + MIN_USABLESIZE_FOR_ENTRYLOG_CREATION + "`")
            .validator(RangeValidator.atLeast(0L))
            .defaultValueSupplier(conf -> MIN_USABLESIZE_FOR_ENTRYLOG_CREATION_KEY.getLong(conf))
            .group(GROUP_LEDGER_STORAGE)
            .orderInGroup(6)
            .build();

    protected static final String FLUSH_INTERVAL = "flushInterval";
    protected static final ConfigKey FLUSH_INTERVAL_KEY = ConfigKey.builder(FLUSH_INTERVAL)
        .type(Type.INT)
        .description("Interval that a bookie flushes ledger storage to disk, in milliseconds")
        .documentation("When `entryLogPerLedgerEnabled` is enabled, checkpoint doesn't happen"
            + " when a new active entrylog is created / previous one is rolled over."
            + " Instead SyncThread checkpoints periodically with 'flushInterval' delay"
            + " (in milliseconds) in between executions. Checkpoint flushes both ledger"
            + " entryLogs and ledger index pages to disk. \n"
            + "Flushing entrylog and index files will introduce much random disk I/O."
            + " If separating journal dir and ledger dirs each on different devices,"
            + " flushing would not affect performance. But if putting journal dir"
            + " and ledger dirs on same device, performance degrade significantly"
            + " on too frequent flushing. You can consider increment flush interval"
            + " to get better performance, but you need to pay more time on bookie"
            + " server restart after failure.\n"
            + "This config is used only when entryLogPerLedgerEnabled is enabled or"
            + " `DbLedgerStorage` is used.")
        .defaultValue(10000)
        .validator(RangeValidator.atLeast(0))
        .group(GROUP_LEDGER_STORAGE)
        .orderInGroup(7)
        .build();

    protected static final String ALLOW_STORAGE_EXPANSION = "allowStorageExpansion";
    protected static final ConfigKey ALLOW_STORAGE_EXPANSION_KEY = ConfigKey.builder(ALLOW_STORAGE_EXPANSION)
        .type(Type.BOOLEAN)
        .description("Allow the expansion of bookie storage capacity")
        .documentation("Newly added ledger and index dirs must be empty")
        .defaultValue(false)
        .group(GROUP_LEDGER_STORAGE)
        .orderInGroup(8)
        .build();

    //
    // Entry Logger Compaction Settings
    //

    private static final ConfigKeyGroup GROUP_ENTRY_LOGGER_COMPACTION = ConfigKeyGroup.builder("compaction")
        .description("Entry log compaction related settings")
        .order(200)
        .build();

    protected static final String COMPACTION_RATE = "compactionRate";
    protected static final String COMPACTION_RATE_BY_ENTRIES = "compactionRateByEntries";
    protected static final String COMPACTION_RATE_BY_BYTES = "compactionRateByBytes";

    protected static final ConfigKey COMPACTION_RATE_KEY = ConfigKey.builder(COMPACTION_RATE)
        .type(Type.INT)
        .description("Set the rate at which compaction will readd entries. The unit is adds per second.")
        .defaultValue(1000)
        .group(GROUP_ENTRY_LOGGER_COMPACTION)
        .orderInGroup(0)
        .deprecated(true)
        .deprecatedByConfigKey(COMPACTION_RATE_BY_ENTRIES)
        .build();
    protected static final ConfigKey COMPACTION_RATE_BY_ENTRIES_KEY = ConfigKey.builder(COMPACTION_RATE_BY_ENTRIES)
        .type(Type.INT)
        .description("Set the rate at which compaction will readd entries. The unit is adds per second.")
        .defaultValueSupplier(conf -> COMPACTION_RATE_KEY.getInt(conf))
        .group(GROUP_ENTRY_LOGGER_COMPACTION)
        .orderInGroup(1)
        .build();
    protected static final ConfigKey COMPACTION_RATE_BY_BYTES_KEY = ConfigKey.builder(COMPACTION_RATE_BY_BYTES)
        .type(Type.INT)
        .description("Set the rate at which compaction will readd entries. The unit is bytes per second.")
        .defaultValue(1000000)
        .group(GROUP_ENTRY_LOGGER_COMPACTION)
        .orderInGroup(2)
        .build();

    protected static final String IS_THROTTLE_BY_BYTES = "isThrottleByBytes";
    protected static final ConfigKey IS_THROTTLE_BY_BYTES_KEY = ConfigKey.builder(IS_THROTTLE_BY_BYTES)
        .type(Type.BOOLEAN)
        .description("Throttle compaction by bytes or by entries.")
        .defaultValue(false)
        .group(GROUP_ENTRY_LOGGER_COMPACTION)
        .orderInGroup(3)
        .build();

    protected static final String COMPACTION_MAX_OUTSTANDING_REQUESTS = "compactionMaxOutstandingRequests";
    protected static final ConfigKey COMPACTION_MAX_OUTSTANDING_REQUESTS_KEY =
        ConfigKey.builder(COMPACTION_MAX_OUTSTANDING_REQUESTS)
            .type(Type.INT)
            .description("Set the maximum number of entries which can be compacted without flushing")
            .documentation("When compacting, the entries are written to the entrylog and the new offsets"
                + " are cached in memory. Once the entrylog is flushed the index is updated with the new"
                + " offsets. This parameter controls the number of entries added to the entrylog before"
                + " a flush is forced. A higher value for this parameter means more memory will be used"
                + " for offsets. Each offset consists of 3 longs. This parameter should _not_ be modified"
                + " unless you know what you're doing.")
            .defaultValue(100000)
            .group(GROUP_ENTRY_LOGGER_COMPACTION)
            .orderInGroup(4)
            .build();

    protected static final String USE_TRANSACTIONAL_COMPACTION = "useTransactionalCompaction";
    protected static final ConfigKey USE_TRANSACTIONAL_COMPACTION_KEY = ConfigKey.builder(USE_TRANSACTIONAL_COMPACTION)
        .type(Type.BOOLEAN)
        .description("Flag to enable/disable transactional compaction")
        .documentation("If it is set to true, it will use transactional compaction, which it will"
            + " use new entry log files to store compacted entries during compaction; if it is set"
            + " to false, it will use normal compaction, which it shares same entry log file with"
            + " normal add operations")
        .defaultValue(false)
        .group(GROUP_ENTRY_LOGGER_COMPACTION)
        .orderInGroup(5)
        .build();

    protected static final String MINOR_COMPACTION_INTERVAL = "minorCompactionInterval";
    protected static final ConfigKey MINOR_COMPACTION_INTERVAL_KEY = ConfigKey.builder(MINOR_COMPACTION_INTERVAL)
        .type(Type.LONG)
        .description("Interval to run minor compaction, in seconds")
        .documentation("If it is set to less than zero, the minor compaction is disabled")
        .defaultValue(3600)
        .group(GROUP_ENTRY_LOGGER_COMPACTION)
        .orderInGroup(6)
        .build();

    protected static final String MINOR_COMPACTION_THRESHOLD = "minorCompactionThreshold";
    protected static final ConfigKey MINOR_COMPACTION_THRESHOLD_KEY = ConfigKey.builder(MINOR_COMPACTION_THRESHOLD)
        .type(Type.DOUBLE)
        .description("Threshold of minor compaction")
        .documentation("For those entry log files whose remaining size percentage reaches below"
            + " this threshold will be compacted in a minor compaction. If it is set to less than zero,"
            + " the minor compaction is disabled.")
        .defaultValue(0.2f)
        .group(GROUP_ENTRY_LOGGER_COMPACTION)
        .orderInGroup(7)
        .build();

    protected static final String MAJOR_COMPACTION_INTERVAL = "majorCompactionInterval";
    protected static final ConfigKey MAJOR_COMPACTION_INTERVAL_KEY = ConfigKey.builder(MAJOR_COMPACTION_INTERVAL)
        .type(Type.LONG)
        .description("Interval to run major compaction, in seconds")
        .documentation("If it is set to less than zero, the major compaction is disabled")
        .defaultValue(86400)
        .group(GROUP_ENTRY_LOGGER_COMPACTION)
        .orderInGroup(8)
        .build();

    protected static final String MAJOR_COMPACTION_THRESHOLD = "majorCompactionThreshold";
    protected static final ConfigKey MAJOR_COMPACTION_THRESHOLD_KEY = ConfigKey.builder(MAJOR_COMPACTION_THRESHOLD)
        .type(Type.DOUBLE)
        .description("Threshold of major compaction")
        .documentation("For those entry log files whose remaining size percentage reaches below"
            + " this threshold will be compacted in a major compaction."
            + " Those entry log files whose remaining size percentage is still"
            + " higher than the threshold will never be compacted."
            + " If it is set to less than zero, the major compaction is disabled.")
        .defaultValue(0.8f)
        .group(GROUP_ENTRY_LOGGER_COMPACTION)
        .orderInGroup(9)
        .build();

    //
    // Garbage Collection Settings
    //

    private static final ConfigKeyGroup GROUP_GC = ConfigKeyGroup.builder("gc")
        .description("Garbage collection related settings")
        .order(210)
        .build();

    protected static final String GC_WAIT_TIME = "gcWaitTime";
    protected static final ConfigKey GC_WAIT_TIME_KEY = ConfigKey.builder(GC_WAIT_TIME)
        .type(Type.LONG)
        .description("How long the interval to trigger next garbage collection, in milliseconds")
        .documentation("Since garbage collection is running in background, too frequent gc will"
            + " heart performance. It is better to give a higher number of gc interval if"
            + " there is enough disk capacity.")
        .defaultValue(600000)
        .group(GROUP_GC)
        .orderInGroup(0)
        .build();

    protected static final String GC_OVERREPLICATED_LEDGER_WAIT_TIME = "gcOverreplicatedLedgerWaitTime";
    protected static final ConfigKey GC_OVERREPLICATED_LEDGER_WAIT_TIME_KEY =
        ConfigKey.builder(GC_OVERREPLICATED_LEDGER_WAIT_TIME)
            .type(Type.LONG)
            .description("How long the interval to trigger next garbage collection of overreplicated"
                + " ledgers, in milliseconds")
            .documentation("This should not be run very frequently since we read the metadata for all"
                + " the ledgers on the bookie from zk")
            .defaultValue(TimeUnit.DAYS.toMillis(1))
            .group(GROUP_GC)
            .orderInGroup(1)
            .build();

    protected static final String IS_FORCE_GC_ALLOW_WHEN_NO_SPACE = "isForceGCAllowWhenNoSpace";
    protected static final ConfigKey IS_FORCE_GC_ALLOW_WHEN_NO_SPACE_KEY =
        ConfigKey.builder(IS_FORCE_GC_ALLOW_WHEN_NO_SPACE)
            .type(Type.BOOLEAN)
            .description("Whether force compaction is allowed when the disk is full or almost full")
            .documentation("Forcing GC may get some space back, but may also fill up disk space more"
                + " quickly. This is because new log files are created before GC, while old garbage"
                + " log files are deleted after GC.")
            .defaultValue(false)
            .group(GROUP_GC)
            .orderInGroup(2)
            .build();

    protected static final String VERIFY_METADATA_ON_GC = "verifyMetadataOnGC";
    protected static final ConfigKey VERIFY_METADATA_ON_GC_KEY = ConfigKey.builder(VERIFY_METADATA_ON_GC)
        .type(Type.BOOLEAN)
        .description("True if the bookie should double check readMetadata prior to gc")
        .defaultValue(false)
        .group(GROUP_GC)
        .orderInGroup(3)
        .build();

    //
    // Disk Utilization Settings
    //

    private static final ConfigKeyGroup GROUP_DISK = ConfigKeyGroup.builder("disk")
        .description("Disk related settings")
        .order(220)
        .build();

    protected static final String DISK_USAGE_THRESHOLD = "diskUsageThreshold";
    protected static final ConfigKey DISK_USAGE_THRESHOLD_KEY = ConfigKey.builder(DISK_USAGE_THRESHOLD)
        .type(Type.FLOAT)
        .description("For each ledger dir, maximum disk space which can be used")
        .documentation("Default is 0.95f. i.e. 95% of disk can be used at most after which nothing will"
            + " be written to that partition. If all ledger dir partions are full, then bookie will turn"
            + " to readonly mode if 'readOnlyModeEnabled=true' is set, else it will shutdown.")
        .defaultValue(0.95f)
        .validator(RangeValidator.between(0f, 1f))
        .group(GROUP_DISK)
        .orderInGroup(0)
        .build();

    protected static final String DISK_USAGE_WARN_THRESHOLD = "diskUsageWarnThreshold";
    protected static final ConfigKey DISK_USAGE_WARN_THRESHOLD_KEY = ConfigKey.builder(DISK_USAGE_WARN_THRESHOLD)
        .type(Type.FLOAT)
        .description("The disk free space warn threshold")
        .documentation("Disk is considered full when usage threshold is exceeded."
            + " Disk returns back to non-full state when usage is below low water mark threshold."
            + " This prevents it from going back and forth between these states frequently when"
            + " concurrent writes and compaction are happening. This also prevent bookie from"
            + " switching frequently between read-only and read-writes states in the same cases.")
        .defaultValue(0.95f)
        .validator(RangeValidator.between(0f, 1f))
        .group(GROUP_DISK)
        .orderInGroup(1)
        .build();

    protected static final String DISK_USAGE_LWM_THRESHOLD = "diskUsageLwmThreshold";
    protected static final ConfigKey DISK_USAGE_LWM_THRESHOLD_KEY = ConfigKey.builder(DISK_USAGE_LWM_THRESHOLD)
        .type(Type.FLOAT)
        .description("The disk free space low water mark threshold")
        .documentation("Disk is considered full when usage threshold is exceeded."
            + " Disk returns back to non-full state when usage is below low water"
            + " mark threshold. This prevents it from going back and forth between"
            + " these states frequently when concurrent writes and compaction are "
            + " happening. This also prevent bookie from switching frequently between"
            + " read-only and read-writes states in the same cases.")
        .defaultValueSupplier((conf) -> DISK_USAGE_THRESHOLD_KEY.getFloat(conf))
        .validator(RangeValidator.between(0f, 1f))
        .group(GROUP_DISK)
        .orderInGroup(2)
        .build();

    protected static final String DISK_CHECK_INTERVAL = "diskCheckInterval";
    protected static final ConfigKey DISK_CHECK_INTERVAL_KEY = ConfigKey.builder(DISK_CHECK_INTERVAL)
        .type(Type.INT)
        .description("Disk check interval in milli seconds")
        .documentation("Interval to check the ledger dirs usage. Default is 10000")
        .defaultValue(TimeUnit.SECONDS.toMillis(10))
        .validator(RangeValidator.atLeast(0))
        .group(GROUP_DISK)
        .orderInGroup(3)
        .build();

    //
    // Disk Scrub Settings
    //

    private static final ConfigKeyGroup GROUP_SCRUB = ConfigKeyGroup.builder("scrub")
        .description("Local Scrub related settings")
        .order(230)
        .build();

    protected static final String LOCAL_SCRUB_PERIOD = "localScrubInterval";
    protected static final ConfigKey LOCAL_SCRUB_PERIOD_KEY = ConfigKey.builder(LOCAL_SCRUB_PERIOD)
        .type(Type.LONG)
        .description("Set local scrub period in seconds (<= 0 for disabled)")
        .documentation("Scrub will be scheduled at delays chosen from the interval (.5 * interval, 1.5 * interval)")
        .defaultValue(0)
        .group(GROUP_SCRUB)
        .orderInGroup(0)
        .build();

    protected static final String LOCAL_SCRUB_RATE_LIMIT = "localScrubRateLimit";
    protected static final ConfigKey LOCAL_SCRUB_RATE_LIMIT_KEY = ConfigKey.builder(LOCAL_SCRUB_RATE_LIMIT)
        .type(Type.DOUBLE)
        .description("local scrub rate limit (entries/second)")
        .defaultValue(60)
        .group(GROUP_SCRUB)
        .orderInGroup(1)
        .build();

    protected static final String LOCAL_CONSISTENCY_CHECK_ON_STARTUP = "localConsistencyCheckOnStartup";
    protected static final ConfigKey LOCAL_CONSISTENCY_CHECK_ON_STARTUP_KEY =
        ConfigKey.builder(LOCAL_CONSISTENCY_CHECK_ON_STARTUP)
            .type(Type.BOOLEAN)
            .description("True if a local consistency check should be performed on startup.")
            .defaultValue(false)
            .group(GROUP_SCRUB)
            .orderInGroup(2)
            .build();

    //
    // Sorted Ledger Storage Settings
    //

    private static final ConfigKeyGroup GROUP_SORTED_LEDGER_STORAGE = ConfigKeyGroup.builder("sortedledgerstorage")
        .description("SortedLedgerStorage related settings")
        .order(240)
        .build();

    protected static final String SKIP_LIST_SIZE_LIMIT = "skipListSizeLimit";
    protected static final ConfigKey SKIP_LIST_SIZE_LIMIT_KEY = ConfigKey.builder(SKIP_LIST_SIZE_LIMIT)
        .type(Type.LONG)
        .description("The skip list data size limitation in EntryMemTable")
        .defaultValue(64 * 1024 * 1024L)
        .validator(RangeValidator.between(0, (Integer.MAX_VALUE - 1) / 2))
        .group(GROUP_SORTED_LEDGER_STORAGE)
        .orderInGroup(100)
        .build();

    protected static final String SKIP_LIST_CHUNK_SIZE_ENTRY = "skipListArenaChunkSize";
    protected static final ConfigKey SKIP_LIST_CHUNK_SIZE_ENTRY_KEY = ConfigKey.builder(SKIP_LIST_CHUNK_SIZE_ENTRY)
        .type(Type.INT)
        .description("The number of bytes we should use as chunk allocation for"
            + " org.apache.bookkeeper.bookie.SkipListArena")
        .defaultValue(4096 * 1024)
        .group(GROUP_SORTED_LEDGER_STORAGE)
        .orderInGroup(101)
        .build();

    protected static final String SKIP_LIST_MAX_ALLOC_ENTRY = "skipListArenaMaxAllocSize";
    protected static final ConfigKey SKIP_LIST_MAX_ALLOC_ENTRY_KEY = ConfigKey.builder(SKIP_LIST_MAX_ALLOC_ENTRY)
        .type(Type.INT)
        .description("The max size we should allocate from the skiplist arena")
        .documentation("Allocations larger than this should be allocated directly by the VM to avoid fragmentation.")
        .defaultValue(128 * 1024)
        .group(GROUP_SORTED_LEDGER_STORAGE)
        .orderInGroup(102)
        .build();

    protected static final String OPEN_FILE_LIMIT = "openFileLimit";
    protected static final ConfigKey OPEN_FILE_LIMIT_KEY = ConfigKey.builder(OPEN_FILE_LIMIT)
        .type(Type.INT)
        .description("Max number of ledger index files could be opened in bookie server")
        .documentation("If number of ledger index files reaches this limitation, bookie"
            + " server started to swap some ledgers from memory to disk. Too frequent swap"
            + " will affect performance. You can tune this number to gain performance according your requirements.")
        .defaultValue(20000)
        .group(GROUP_SORTED_LEDGER_STORAGE)
        .orderInGroup(103)
        .build();

    protected static final String FILEINFO_CACHE_INITIAL_CAPACITY = "fileInfoCacheInitialCapacity";
    protected static final ConfigKey FILEINFO_CACHE_INITIAL_CAPACITY_KEY =
        ConfigKey.builder(FILEINFO_CACHE_INITIAL_CAPACITY)
            .type(Type.INT)
            .description("The minimum total size of the internal file info cache table")
            .documentation("Providing a large enough estimate at construction time avoids the need"
                + " for expensive resizing operations later, but setting this value unnecessarily"
                + " high wastes memory. The default value is `1/4` of # `" + OPEN_FILE_LIMIT + "`"
                + " if " + OPEN_FILE_LIMIT + " is positive, otherwise it is 64.")
            .defaultValueSupplier(conf -> Math.max(OPEN_FILE_LIMIT_KEY.getInt(conf) / 4, 64))
            .group(GROUP_SORTED_LEDGER_STORAGE)
            .orderInGroup(104)
            .build();

    protected static final String FILEINFO_MAX_IDLE_TIME = "fileInfoMaxIdleTime";
    protected static final ConfigKey FILEINFO_MAX_IDLE_TIME_KEY = ConfigKey.builder(FILEINFO_MAX_IDLE_TIME)
        .type(Type.LONG)
        .description("The max idle time allowed for an open file info existed in the file info cache")
        .documentation("If the file info is idle for a long time, exceed the given time period. The file info"
            + " will be evicted and closed. If the value is zero or negative, the file info is evicted"
            + " only when opened files reached openFileLimit.")
        .defaultValue(0)
        .group(GROUP_SORTED_LEDGER_STORAGE)
        .orderInGroup(105)
        .build();

    protected static final String FILEINFO_FORMAT_VERSION_TO_WRITE = "fileInfoFormatVersionToWrite";
    protected static final ConfigKey FILEINFO_FORMAT_VERSION_TO_WRITE_KEY =
        ConfigKey.builder(FILEINFO_FORMAT_VERSION_TO_WRITE)
            .type(Type.INT)
            .description("The fileinfo format version to write")
            .documentation("If you'd like to disable persisting ExplicitLac, you can set this config to 0 and"
                + " also journalFormatVersionToWrite should be set to < 6. If there is mismatch then the"
                + " server config is considered invalid")
            .optionValues(Lists.newArrayList(
                "0: Initial version",
                "1: persisting explicitLac is introduced"
            ))
            .defaultValue(1)
            .group(GROUP_SORTED_LEDGER_STORAGE)
            .orderInGroup(106)
            .build();

    protected static final String PAGE_SIZE = "pageSize";
    protected static final ConfigKey PAGE_SIZE_KEY = ConfigKey.builder(PAGE_SIZE)
        .type(Type.INT)
        .description("Size of a index page in ledger cache, in bytes")
        .documentation("A larger index page can improve performance writing page to disk, which is efficent"
            + " when you have small number of ledgers and these ledgers have similar number of entries. If"
            + " you have large number of ledgers and each ledger has fewer entries, smaller index page would"
            + " improve memory usage.")
        .defaultValue(8192)
        .group(GROUP_SORTED_LEDGER_STORAGE)
        .orderInGroup(107)
        .build();

    protected static final String PAGE_LIMIT = "pageLimit";
    protected static final ConfigKey PAGE_LIMIT_KEY = ConfigKey.builder(PAGE_LIMIT)
        .type(Type.INT)
        .description("How many index pages provided in ledger cache")
        .documentation("If number of index pages reaches this limitation, bookie server starts to swap some ledgers"
            + " from memory to disk. You can increment this value when you found swap became more frequent."
            + " But make sure pageLimit*pageSize should not more than JVM max memory limitation, otherwise you would"
            + " got OutOfMemoryException. In general, incrementing pageLimit, using smaller index page would gain"
            + " bettern performance in lager number of ledgers with fewer entries case If pageLimit is -1, bookie"
            + " server will use 1/3 of JVM memory to compute the limitation of number of index pages.")
        .defaultValue(-1)
        .group(GROUP_SORTED_LEDGER_STORAGE)
        .orderInGroup(108)
        .build();

    //
    // Db Ledger Storage Settings
    //

    private static final ConfigKeyGroup GROUP_DB_LEDGER_STORAGE = ConfigKeyGroup.builder("dbledgerstorage")
        .description("DbLedgerStorage related settings")
        .order(250)
        .build();


    //
    // ZK metadata service settings
    //

    protected static final String ZK_RETRY_BACKOFF_START_MS = "zkRetryBackoffStartMs";
    protected static final ConfigKey ZK_RETRY_BACKOFF_START_MS_KEY = ConfigKey.builder(ZK_RETRY_BACKOFF_START_MS)
        .type(Type.INT)
        .description("The Zookeeper client backoff retry start time in millis")
        .defaultValueSupplier(conf -> ZK_TIMEOUT_KEY.getInt(conf))
        .group(GROUP_ZK)
        .orderInGroup(100)
        .build();

    protected static final String ZK_RETRY_BACKOFF_MAX_MS = "zkRetryBackoffMaxMs";
    protected static final ConfigKey ZK_RETRY_BACKOFF_MAX_MS_KEY = ConfigKey.builder(ZK_RETRY_BACKOFF_MAX_MS)
        .type(Type.INT)
        .description("The Zookeeper client backoff retry max time in millis")
        .defaultValueSupplier(conf -> ZK_TIMEOUT_KEY.getInt(conf))
        .group(GROUP_ZK)
        .orderInGroup(101)
        .build();

    //
    // Statistics Settings
    //

    private static final ConfigKeyGroup GROUP_STATS = ConfigKeyGroup.builder("stats")
        .description("Stats related settings")
        .order(260)
        .children(Lists.newArrayList(
            "org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider",
            "org.apache.bookkeeper.stats.codahale.CodahaleMetricsProvider",
            "org.apache.bookkeeper.stats.twitter.finagle.FinagleStatsProvider",
            "org.apache.bookkeeper.stats.twitter.ostrich.OstrichProvider",
            "org.apache.bookkeeper.stats.twitter.science.TwitterStatsProvider"
        ))
        .build();

    protected static final String ENABLE_STATISTICS = "enableStatistics";
    protected static final ConfigKey ENABLE_STATISTICS_KEY = ConfigKey.builder(ENABLE_STATISTICS)
        .type(Type.BOOLEAN)
        .description("Turn on/off statistics")
        .defaultValue(true)
        .group(GROUP_STATS)
        .orderInGroup(100)
        .build();

    protected static final String ENABLE_TASK_EXECUTION_STATS = "enableTaskExecutionStats";
    protected static final ConfigKey ENABLE_TASK_EXECUTION_STATS_KEY = ConfigKey.builder(ENABLE_TASK_EXECUTION_STATS)
        .type(Type.BOOLEAN)
        .description("The flag to enable recording task execution stats")
        .defaultValue(false)
        .group(GROUP_STATS)
        .orderInGroup(101)
        .build();

    protected static final String STATS_PROVIDER_CLASS = "statsProviderClass";
    protected static final ConfigKey STATS_PROVIDER_CLASS_KEY = ConfigKey.builder(STATS_PROVIDER_CLASS)
        .type(Type.CLASS)
        .description("Stats Provider Class (if `" + ENABLE_STATISTICS + "` are enabled)")
        .defaultValue(NullStatsProvider.class)
        .optionValues(Lists.newArrayList(
            "Prometheus        : org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider",
            "Codahale          : org.apache.bookkeeper.stats.codahale.CodahaleMetricsProvider",
            "Twitter Finagle   : org.apache.bookkeeper.stats.twitter.finagle.FinagleStatsProvider",
            "Twitter Ostrich   : org.apache.bookkeeper.stats.twitter.ostrich.OstrichProvider",
            "Twitter Science   : org.apache.bookkeeper.stats.twitter.science.TwitterStatsProvider"
        ))
        .group(GROUP_STATS)
        .orderInGroup(102)
        .build();

    //
    // Replication Worker Settings
    //

    protected static final String OPEN_LEDGER_REREPLICATION_GRACE_PERIOD = "openLedgerRereplicationGracePeriod";
    protected static final ConfigKey OPEN_LEDGER_REREPLICATION_GRACE_PERIOD_KEY =
        ConfigKey.builder(OPEN_LEDGER_REREPLICATION_GRACE_PERIOD)
            .type(Type.INT)
            .description("The grace period, in seconds, that the replication worker waits"
                    + " before fencing and replicating a ledger fragment that's still being"
                    + " written to upon bookie failure.")
            .defaultValue(30)
            .group(GROUP_REPLICATION_WORKER)
            .orderInGroup(100)
            .build();

    protected static final String RW_REREPLICATE_BACKOFF_MS = "rwRereplicateBackoffMs";
    protected static final ConfigKey RW_REREPLICATE_BACKOFF_MS_KEY =
        ConfigKey.builder(RW_REREPLICATE_BACKOFF_MS)
            .type(Type.INT)
            .description("The time to backoff when replication worker encounters exceptions on"
                + " replicating a ledger, in milliseconds")
            .defaultValue(5000)
            .group(GROUP_REPLICATION_WORKER)
            .orderInGroup(101)
            .build();

    protected static final String LOCK_RELEASE_OF_FAILED_LEDGER_GRACE_PERIOD = "lockReleaseOfFailedLedgerGracePeriod";
    protected static final ConfigKey LOCK_RELEASE_OF_FAILED_LEDGER_GRACE_PERIOD_KEY =
        ConfigKey.builder(LOCK_RELEASE_OF_FAILED_LEDGER_GRACE_PERIOD)
            .type(Type.INT)
            .description("The grace period, in milliseconds, if the replication worker fails to replicate"
                + " a underreplicatedledger for more than `10` times, then instead of releasing the lock"
                + " immediately after failed attempt, it will hold under replicated ledger lock for this"
                + " grace period and then it will release the lock.")
            .defaultValue(60000)
            .group(GROUP_REPLICATION_WORKER)
            .orderInGroup(102)
            .build();

    //
    // Auditor Settings
    //

    protected static final String AUDITOR_PERIODIC_CHECK_INTERVAL = "auditorPeriodicCheckInterval";
    protected static final ConfigKey AUDITOR_PERIODIC_CHECK_INTERVAL_KEY =
        ConfigKey.builder(AUDITOR_PERIODIC_CHECK_INTERVAL)
            .type(Type.INT)
            .description("The interval between auditor bookie checks, in seconds")
            .documentation("The auditor bookie check, checks ledger metadata to see which bookies should"
                + " contain entries for each ledger. If a bookie which should contain entries is "
                + " unavailable, then the ledger containing that entry is marked for recovery."
                + " Setting this to 0 disabled the periodic check. Bookie checks will still run when"
                + " a bookie fails")
            .defaultValue(604800)
            .group(GROUP_AUDITOR)
            .orderInGroup(100)
            .build();

    protected static final String AUDITOR_PERIODIC_BOOKIE_CHECK_INTERVAL = "auditorPeriodicBookieCheckInterval";
    protected static final ConfigKey AUDITOR_PERIODIC_BOOKIE_CHECK_INTERVAL_KEY =
        ConfigKey.builder(AUDITOR_PERIODIC_BOOKIE_CHECK_INTERVAL)
            .type(Type.INT)
            .description("Interval at which the auditor will do a check of all ledgers in the cluster, in seconds")
            .documentation("To disable the periodic check completely, set this to 0. Note that periodic checking"
                    + " will put extra load on the cluster, so it should not be run more frequently than once a day.")
            .defaultValue(86400)
            .group(GROUP_AUDITOR)
            .orderInGroup(101)
            .build();

    protected static final String AUDITOR_LEDGER_VERIFICATION_PERCENTAGE = "auditorLedgerVerificationPercentage";
    protected static final ConfigKey AUDITOR_LEDGER_VERIFICATION_PERCENTAGE_KEY =
        ConfigKey.builder(AUDITOR_LEDGER_VERIFICATION_PERCENTAGE)
            .type(Type.INT)
            .description("The percentage of a ledger (fragment)'s entries will be verified before claiming this"
                + " fragment as missing fragment")
            .documentation("Default is 0, which only verify the first and last entries of a given fragment")
            .defaultValue(0)
            .group(GROUP_AUDITOR)
            .orderInGroup(102)
            .build();

    protected static final String LOST_BOOKIE_RECOVERY_DELAY = "lostBookieRecoveryDelay";
    protected static final ConfigKey LOST_BOOKIE_RECOVERY_DELAY_KEY = ConfigKey.builder(LOST_BOOKIE_RECOVERY_DELAY)
        .type(Type.INT)
        .description("How long to wait, in seconds, before starting auto recovery of a lost bookie")
        .defaultValue(0)
        .group(GROUP_AUDITOR)
        .orderInGroup(103)
        .build();

    //
    // AutoRecovery Settings
    //

    protected static final String AUTO_RECOVERY_DAEMON_ENABLED = "autoRecoveryDaemonEnabled";
    protected static final ConfigKey AUTO_RECOVERY_DAEMON_ENABLED_KEY =
        ConfigKey.builder(AUTO_RECOVERY_DAEMON_ENABLED)
            .type(Type.BOOLEAN)
            .description("Whether the bookie itself can start auto-recovery service also or not")
            .defaultValue(false)
            .group(GROUP_AUTORECOVERY)
            .orderInGroup(100)
            .build();

    /**
     * Construct a default configuration object.
     */
    public ServerConfiguration() {
        super();
    }

    /**
     * Construct a configuration based on other configuration.
     *
     * @param conf
     *          Other configuration
     */
    public ServerConfiguration(AbstractConfiguration conf) {
        super();
        loadConf(conf);
    }

    /**
     * Get entry logger size limitation.
     *
     * @return entry logger size limitation
     */
    public long getEntryLogSizeLimit() {
        return ENTRY_LOG_SIZE_LIMIT_KEY.getLong(this);
    }

    /**
     * Set entry logger size limitation.
     *
     * @param logSizeLimit
     *          new log size limitation
     */
    public ServerConfiguration setEntryLogSizeLimit(long logSizeLimit) {
        ENTRY_LOG_SIZE_LIMIT_KEY.set(this, logSizeLimit);
        return this;
    }

    /**
     * Is entry log file preallocation enabled.
     *
     * @return whether entry log file preallocation is enabled or not.
     */
    public boolean isEntryLogFilePreAllocationEnabled() {
        return ENTRY_LOG_FILE_PREALLOCATION_ENABLED_KEY.getBoolean(this);
    }

    /**
     * Enable/disable entry log file preallocation.
     *
     * @param enabled
     *          enable/disable entry log file preallocation.
     * @return server configuration object.
     */
    public ServerConfiguration setEntryLogFilePreAllocationEnabled(boolean enabled) {
        ENTRY_LOG_FILE_PREALLOCATION_ENABLED_KEY.set(this, enabled);
        return this;
    }

    /**
     * Get Garbage collection wait time. Default value is 10 minutes.
     * The guideline is not to set a too low value for this, if using zookeeper based
     * ledger manager. And it would be nice to align with the average lifecyle time of
     * ledgers in the system.
     *
     * @return gc wait time
     */
    public long getGcWaitTime() {
        return GC_WAIT_TIME_KEY.getLong(this);
    }

    /**
     * Set garbage collection wait time.
     *
     * @param gcWaitTime
     *          gc wait time
     * @return server configuration
     */
    public ServerConfiguration setGcWaitTime(long gcWaitTime) {
        GC_WAIT_TIME_KEY.set(this, gcWaitTime);
        return this;
    }

    /**
     * Get wait time in millis for garbage collection of overreplicated ledgers.
     *
     * @return gc wait time
     */
    public long getGcOverreplicatedLedgerWaitTimeMillis() {
        return GC_OVERREPLICATED_LEDGER_WAIT_TIME_KEY.getLong(this);
    }

    /**
     * Set wait time for garbage collection of overreplicated ledgers. Default: 1 day
     *
     * <p>A ledger can be overreplicated under the following circumstances:
     * 1. The ledger with few entries has bk1 and bk2 as its ensemble.
     * 2. bk1 crashes.
     * 3. bk3 replicates the ledger from bk2 and updates the ensemble to bk2 and bk3.
     * 4. bk1 comes back up.
     * 5. Now there are 3 copies of the ledger.
     *
     * @param gcWaitTime
     * @return server configuration
     */
    public ServerConfiguration setGcOverreplicatedLedgerWaitTime(long gcWaitTime, TimeUnit unit) {
        GC_OVERREPLICATED_LEDGER_WAIT_TIME_KEY.set(this, gcWaitTime);
        return this;
    }

    /**
     * Get whether to use transactional compaction and using a separate log for compaction or not.
     *
     * @return use transactional compaction
     */
    public boolean getUseTransactionalCompaction() {
        return USE_TRANSACTIONAL_COMPACTION_KEY.getBoolean(this);
    }

    /**
     * Set whether to use transactional compaction and using a separate log for compaction or not.
     * @param useTransactionalCompaction
     * @return server configuration
     */
    public ServerConfiguration setUseTransactionalCompaction(boolean useTransactionalCompaction) {
        USE_TRANSACTIONAL_COMPACTION_KEY.set(this, useTransactionalCompaction);
        return this;
    }

    /**
     * Get whether the bookie is configured to double check prior to gc.
     *
     * @return use transactional compaction
     */
    public boolean getVerifyMetadataOnGC() {
        return VERIFY_METADATA_ON_GC_KEY.getBoolean(this);
    }

    /**
     * Set whether the bookie is configured to double check prior to gc.
     * @param verifyMetadataOnGC
     * @return server configuration
     */
    public ServerConfiguration setVerifyMetadataOnGc(boolean verifyMetadataOnGC) {
        VERIFY_METADATA_ON_GC_KEY.set(this, verifyMetadataOnGC);
        return this;
    }

    /**
     * Get whether local scrub is enabled.
     *
     * @return Whether local scrub is enabled.
     */
    public boolean isLocalScrubEnabled() {
        return this.getLocalScrubPeriod() > 0;
    }

    /**
     * Get local scrub interval.
     *
     * @return Number of seconds between scrubs, <= 0 for disabled.
     */
    public long getLocalScrubPeriod() {
        return LOCAL_SCRUB_PERIOD_KEY.getLong(this);
    }

    /**
     * Set local scrub period in seconds (<= 0 for disabled). Scrub will be scheduled at delays
     * chosen from the interval (.5 * interval, 1.5 * interval)
     */
    public ServerConfiguration setLocalScrubPeriod(long period) {
        LOCAL_SCRUB_PERIOD_KEY.set(this, period);
        return this;
    }

    /**
     * Get local scrub rate limit (entries/second).
     *
     * @return Max number of entries to scrub per second, 0 for disabled.
     */
    public double getLocalScrubRateLimit() {
        return LOCAL_SCRUB_RATE_LIMIT_KEY.getDouble(this);
    }

    /**
     * Get local scrub rate limit (entries/second).
     *
     * @param scrubRateLimit Max number of entries per second to scan.
     */
    public ServerConfiguration setLocalScrubRateLimit(double scrubRateLimit) {
        LOCAL_SCRUB_RATE_LIMIT_KEY.set(this, scrubRateLimit);
        return this;
    }

    /**
     * Get flush interval. Default value is 10 second. It isn't useful to decrease
     * this value, since ledger storage only checkpoints when an entry logger file
     * is rolled.
     *
     * @return flush interval
     */
    public int getFlushInterval() {
        return FLUSH_INTERVAL_KEY.getInt(this);
    }

    /**
     * Set flush interval.
     *
     * @param flushInterval
     *          Flush Interval
     * @return server configuration
     */
    public ServerConfiguration setFlushInterval(int flushInterval) {
        FLUSH_INTERVAL_KEY.set(this, flushInterval);
        return this;
    }

    /**
     * Set entry log flush interval in bytes.
     *
     * <p>Default is 0. 0 or less disables this feature and effectively flush
     * happens on log rotation.
     *
     * <p>Flushing in smaller chunks but more frequently reduces spikes in disk
     * I/O. Flushing too frequently may also affect performance negatively.
     *
     * @return Entry log flush interval in bytes
     */
    public long getFlushIntervalInBytes() {
        return FLUSH_ENTRYLOG_INTERVAL_BYTES_KEY.getLong(this);
    }

    /**
     * Set entry log flush interval in bytes.
     *
     * @param flushInterval in bytes
     * @return server configuration
     */
    public ServerConfiguration setFlushIntervalInBytes(long flushInterval) {
        FLUSH_ENTRYLOG_INTERVAL_BYTES_KEY.set(this, flushInterval);
        return this;
    }

    /**
     * Get bookie death watch interval.
     *
     * @return watch interval
     */
    public int getDeathWatchInterval() {
        return DEATH_WATCH_INTERVAL_KEY.getInt(this);
    }

    /**
     * Get open file limit. Default value is 20000.
     *
     * @return max number of files to open
     */
    public int getOpenFileLimit() {
        return OPEN_FILE_LIMIT_KEY.getInt(this);
    }

    /**
     * Set limitation of number of open files.
     *
     * @param fileLimit
     *          Limitation of number of open files.
     * @return server configuration
     */
    public ServerConfiguration setOpenFileLimit(int fileLimit) {
        OPEN_FILE_LIMIT_KEY.set(this, fileLimit);
        return this;
    }

    /**
     * Get limitation number of index pages in ledger cache.
     *
     * @return max number of index pages in ledger cache
     */
    public int getPageLimit() {
        return PAGE_LIMIT_KEY.getInt(this);
    }

    /**
     * Set limitation number of index pages in ledger cache.
     *
     * @param pageLimit
     *          Limitation of number of index pages in ledger cache.
     * @return server configuration
     */
    public ServerConfiguration setPageLimit(int pageLimit) {
        PAGE_LIMIT_KEY.set(this, pageLimit);
        return this;
    }

    /**
     * Get page size.
     *
     * @return page size in ledger cache
     */
    public int getPageSize() {
        return PAGE_SIZE_KEY.getInt(this);
    }

    /**
     * Set page size.
     *
     * @see #getPageSize()
     *
     * @param pageSize
     *          Page Size
     * @return Server Configuration
     */
    public ServerConfiguration setPageSize(int pageSize) {
        PAGE_SIZE_KEY.set(this, pageSize);
        return this;
    }

    /**
     * Get the minimum total size for the internal file info cache tables.
     * Providing a large enough estimate at construction time avoids the need for
     * expensive resizing operations later, but setting this value unnecessarily high
     * wastes memory.
     *
     * @return minimum size of initial file info cache.
     */
    public int getFileInfoCacheInitialCapacity() {
        return FILEINFO_CACHE_INITIAL_CAPACITY_KEY.getInt(this);
    }

    /**
     * Set the minimum total size for the internal file info cache tables for initialization.
     *
     * @param initialCapacity
     *          Initial capacity of file info cache table.
     * @return server configuration instance.
     */
    public ServerConfiguration setFileInfoCacheInitialCapacity(int initialCapacity) {
        FILEINFO_CACHE_INITIAL_CAPACITY_KEY.set(this, initialCapacity);
        return this;
    }

    /**
     * Get the max idle time allowed for a open file info existed in file info cache.
     * If the file info is idle for a long time, exceed the given time period. The file
     * info will be evicted and closed. If the value is zero, the file info is evicted
     * only when opened files reached openFileLimit.
     *
     * @see #getOpenFileLimit
     * @return max idle time of a file info in the file info cache.
     */
    public long getFileInfoMaxIdleTime() {
        return FILEINFO_MAX_IDLE_TIME_KEY.getLong(this);
    }

    /**
     * Set the max idle time allowed for a open file info existed in file info cache.
     *
     * @param idleTime
     *          Idle time, in seconds.
     * @see #getFileInfoMaxIdleTime
     * @return server configuration object.
     */
    public ServerConfiguration setFileInfoMaxIdleTime(long idleTime) {
        FILEINFO_MAX_IDLE_TIME_KEY.set(this, idleTime);
        return this;
    }

    /**
     * Get fileinfo format version to write.
     *
     * @return fileinfo format version to write.
     */
    public int getFileInfoFormatVersionToWrite() {
        return FILEINFO_FORMAT_VERSION_TO_WRITE_KEY.getInt(this);
    }

    /**
     * Set fileinfo format version to write.
     *
     * @param version
     *            fileinfo format version to write.
     * @return server configuration.
     */
    public ServerConfiguration setFileInfoFormatVersionToWrite(int version) {
        FILEINFO_FORMAT_VERSION_TO_WRITE_KEY.set(this, version);
        return this;
    }

    /**
     * Max journal file size.
     *
     * @return max journal file size
     */
    public long getMaxJournalSizeMB() {
        return MAX_JOURNAL_SIZE_KEY.getLong(this);
    }

    /**
     * Set new max journal file size.
     *
     * @param maxJournalSize
     *          new max journal file size
     * @return server configuration
     */
    public ServerConfiguration setMaxJournalSizeMB(long maxJournalSize) {
        MAX_JOURNAL_SIZE_KEY.set(this, maxJournalSize);
        return this;
    }

    /**
     * How much space should we pre-allocate at a time in the journal.
     *
     * @return journal pre-allocation size in MB
     */
    public int getJournalPreAllocSizeMB() {
        return JOURNAL_PRE_ALLOC_SIZE_KEY.getInt(this);
    }

    /**
     * Size of the write buffers used for the journal.
     *
     * @return journal write buffer size in KB
     */
    public int getJournalWriteBufferSizeKB() {
        return JOURNAL_WRITE_BUFFER_SIZE_KEY.getInt(this);
    }

    /**
     * Max number of older journal files kept.
     *
     * @return max number of older journal files to kept
     */
    public int getMaxBackupJournals() {
        return MAX_BACKUP_JOURNALS_KEY.getInt(this);
    }

    /**
     * Set max number of older journal files to kept.
     *
     * @param maxBackupJournals
     *          Max number of older journal files
     * @return server configuration
     */
    public ServerConfiguration setMaxBackupJournals(int maxBackupJournals) {
        MAX_JOURNAL_SIZE_KEY.set(this, maxBackupJournals);
        return this;
    }

    /**
     * All the journal writes and commits should be aligned to given size. If not,
     * zeros will be padded to align to given size.
     *
     * @return journal alignment size
     */
    public int getJournalAlignmentSize() {
        return JOURNAL_ALIGNMENT_SIZE_KEY.getInt(this);
    }

    /**
     * Set journal alignment size.
     *
     * @param size
     *          journal alignment size.
     * @return server configuration.
     */
    public ServerConfiguration setJournalAlignmentSize(int size) {
        JOURNAL_ALIGNMENT_SIZE_KEY.set(this, size);
        return this;
    }

    /**
     * Get journal format version to write.
     *
     * @return journal format version to write.
     */
    public int getJournalFormatVersionToWrite() {
        return JOURNAL_FORMAT_VERSION_TO_WRITE_KEY.getInt(this);
    }

    /**
     * Set journal format version to write.
     *
     * @param version
     *          journal format version to write.
     * @return server configuration.
     */
    public ServerConfiguration setJournalFormatVersionToWrite(int version) {
        JOURNAL_FORMAT_VERSION_TO_WRITE_KEY.set(this, version);
        return this;
    }

    /**
     * Get max number of adds in progress. 0 == unlimited.
     *
     * @return Max number of adds in progress.
     */
    public int getMaxAddsInProgressLimit() {
        return MAX_ADDS_IN_PROGRESS_LIMIT_KEY.getInt(this);
    }

    /**
     * Set max number of adds in progress. 0 == unlimited.
     *
     * @param value
     *          max number of adds in progress.
     * @return server configuration.
     */
    public ServerConfiguration setMaxAddsInProgressLimit(int value) {
        MAX_ADDS_IN_PROGRESS_LIMIT_KEY.set(this, value);
        return this;
    }

    /**
     * Get max number of reads in progress. 0 == unlimited.
     *
     * @return Max number of reads in progress.
     */
    public int getMaxReadsInProgressLimit() {
        return MAX_READS_IN_PROGRESS_LIMIT_KEY.getInt(this);
    }

    /**
     * Set max number of reads in progress. 0 == unlimited.
     *
     * @param value
     *          max number of reads in progress.
     * @return server configuration.
     */
    public ServerConfiguration setMaxReadsInProgressLimit(int value) {
        MAX_READS_IN_PROGRESS_LIMIT_KEY.set(this, value);
        return this;
    }

    /**
     * Configures action in case if server timed out sending response to the client.
     * true == close the channel and drop response
     * false == drop response
     * Requires waitTimeoutOnBackpressureMs >= 0 otherwise ignored.
     *
     * @return value indicating if channel should be closed.
     */
    public boolean getCloseChannelOnResponseTimeout(){
        return CLOSE_CHANNEL_ON_RESPONSE_TIMEOUT_KEY.getBoolean(this);
    }

    /**
     * Configures action in case if server timed out sending response to the client.
     * true == close the channel and drop response
     * false == drop response
     * Requires waitTimeoutOnBackpressureMs >= 0 otherwise ignored.
     *
     * @param value
     * @return server configuration.
     */
    public ServerConfiguration setCloseChannelOnResponseTimeout(boolean value) {
        CLOSE_CHANNEL_ON_RESPONSE_TIMEOUT_KEY.set(this, value);
        return this;
    }

    /**
     * Timeout controlling wait on response send in case of unresponsive client
     * (i.e. client in long GC etc.)
     *
     * @return timeout value
     *        negative value disables the feature
     *        0 to allow request to fail immediately
     *        Default is -1 (disabled)
     */
    public long getWaitTimeoutOnResponseBackpressureMillis() {
        return WAIT_TIMEOUT_ON_RESPONSE_BACKPRESSURE_KEY.getLong(this);
    }

    /**
     * Timeout controlling wait on response send in case of unresponsive client
     * (i.e. client in long GC etc.)
     *
     * @param value
     *        negative value disables the feature
     *        0 to allow request to fail immediately
     *        Default is -1 (disabled)
     * @return client configuration.
     */
    public ServerConfiguration setWaitTimeoutOnResponseBackpressureMillis(long value) {
        WAIT_TIMEOUT_ON_RESPONSE_BACKPRESSURE_KEY.set(this, value);
        return this;
    }

    /**
     * Get bookie port that bookie server listen on.
     *
     * @return bookie port
     */
    public int getBookiePort() {
        return BOOKIE_PORT_KEY.getInt(this);
    }

    /**
     * Set new bookie port that bookie server listen on.
     *
     * @param port
     *          Port to listen on
     * @return server configuration
     */
    public ServerConfiguration setBookiePort(int port) {
        BOOKIE_PORT_KEY.set(this, port);
        return this;
    }

    /**
     * Get the network interface that the bookie should
     * listen for connections on. If this is null, then the bookie
     * will listen for connections on all interfaces.
     *
     * @return the network interface to listen on, e.g. eth0, or
     *         null if none is specified
     */
    public String getListeningInterface() {
        return LISTENING_INTERFACE_KEY.getString(this);
    }

    /**
     * Set the network interface that the bookie should listen on.
     * If not set, the bookie will listen on all interfaces.
     *
     * @param iface the interface to listen on
     */
    public ServerConfiguration setListeningInterface(String iface) {
        LISTENING_INTERFACE_KEY.set(this, iface);
        return this;
    }

    /**
     * Is the bookie allowed to use a loopback interface as its primary
     * interface(i.e. the interface it uses to establish its identity)?
     *
     * <p>By default, loopback interfaces are not allowed as the primary
     * interface.
     *
     * <p>Using a loopback interface as the primary interface usually indicates
     * a configuration error. For example, its fairly common in some VPS setups
     * to not configure a hostname, or to have the hostname resolve to
     * 127.0.0.1. If this is the case, then all bookies in the cluster will
     * establish their identities as 127.0.0.1:3181, and only one will be able
     * to join the cluster. For VPSs configured like this, you should explicitly
     * set the listening interface.
     *
     * @see #setListeningInterface(String)
     * @return whether a loopback interface can be used as the primary interface
     */
    public boolean getAllowLoopback() {
        return ALLOW_LOOPBACK_KEY.getBoolean(this);
    }

    /**
     * Configure the bookie to allow loopback interfaces to be used
     * as the primary bookie interface.
     *
     * @see #getAllowLoopback
     * @param allow whether to allow loopback interfaces
     * @return server configuration
     */
    public ServerConfiguration setAllowLoopback(boolean allow) {
        ALLOW_LOOPBACK_KEY.set(this, allow);
        return this;
    }

    /**
     * Get the configured advertised address for the bookie.
     *
     * <p>If present, this setting will take precedence over the
     * {@link #setListeningInterface(String)} and
     * {@link #setUseHostNameAsBookieID(boolean)}.
     *
     * @see #setAdvertisedAddress(String)
     * @return the configure address to be advertised
     */
    public String getAdvertisedAddress() {
        return ADVERTISED_ADDRESS_KEY.getString(this);
    }

    /**
     * Configure the bookie to advertise a specific address.
     *
     * <p>By default, a bookie will advertise either its own IP or hostname,
     * depending on the {@link getUseHostNameAsBookieID()} setting.
     *
     * <p>When the advertised is set to a non-empty string, the bookie will
     * register and advertise using this address.
     *
     * <p>If present, this setting will take precedence over the
     * {@link #setListeningInterface(String)} and
     * {@link #setUseHostNameAsBookieID(boolean)}.
     *
     * @see #getAdvertisedAddress()
     * @param advertisedAddress
     *            whether to allow loopback interfaces
     * @return server configuration
     */
    public ServerConfiguration setAdvertisedAddress(String advertisedAddress) {
        ADVERTISED_ADDRESS_KEY.set(this, advertisedAddress);
        return this;
    }

    /**
     * Is the bookie allowed to use an ephemeral port (port 0) as its server port.
     *
     * <p>By default, an ephemeral port is not allowed. Using an ephemeral port
     * as the service port usually indicates a configuration error. However, in unit
     * tests, using ephemeral port will address port conflicts problem and allow
     * running tests in parallel.
     *
     * @return whether is allowed to use an ephemeral port.
     */
    public boolean getAllowEphemeralPorts() {
        return ALLOW_EPHEMERAL_PORTS_KEY.getBoolean(this);
    }

    /**
     * Configure the bookie to allow using an ephemeral port.
     *
     * @param allow whether to allow using an ephemeral port.
     * @return server configuration
     */
    public ServerConfiguration setAllowEphemeralPorts(boolean allow) {
        ALLOW_EPHEMERAL_PORTS_KEY.set(this, allow);
        return this;
    }

    /**
     * Return whether we should allow addition of ledger/index dirs to an existing bookie.
     *
     * @return true if the addition is allowed; false otherwise
     */
    public boolean getAllowStorageExpansion() {
        return ALLOW_STORAGE_EXPANSION_KEY.getBoolean(this);
    }

    /**
     * Change the setting of whether or not we should allow ledger/index
     * dirs to be added to the current set of dirs.
     *
     * @param val - true if new ledger/index dirs can be added; false otherwise.
     *
     * @return server configuration
     */
    public ServerConfiguration setAllowStorageExpansion(boolean val) {
        ALLOW_STORAGE_EXPANSION_KEY.set(this, val);
        return this;
    }

    /**
     * Get dir names to store journal files.
     *
     * @return journal dir name
     */
    public String[] getJournalDirNames() {
        String[] journalDirs = JOURNAL_DIRS_KEY.getArray(this);
        if (journalDirs == null || journalDirs.length == 0) {
            return new String[] {getJournalDirName()};
        }
        return journalDirs;
    }

    /**
     * Get dir name to store journal files.
     *
     * @return journal dir name
     */
    @Deprecated
    public String getJournalDirName() {
        return JOURNAL_DIR_KEY.getString(this);
    }

    /**
     * Get dir name to store journal files.
     *
     * @return journal dir name
     */
    public String getJournalDirNameWithoutDefault() {
        return JOURNAL_DIR_KEY.getStringWithoutDefault(this);
    }


    /**
     * Set dir name to store journal files.
     *
     * @param journalDir
     *          Dir to store journal files
     * @return server configuration
     */
    public ServerConfiguration setJournalDirName(String journalDir) {
        JOURNAL_DIRS_KEY.set(this, new String[] { journalDir });
        return this;
    }

    /**
     * Set dir names to store journal files.
     *
     * @param journalDirs
     *          Dir to store journal files
     * @return server configuration
     */
    public ServerConfiguration setJournalDirsName(String[] journalDirs) {
        JOURNAL_DIRS_KEY.set(this, journalDirs);
        return this;
    }

    /**
     * Get dirs to store journal files.
     *
     * @return journal dirs, if no journal dir provided return null
     */
    public File[] getJournalDirs() {
        String[] journalDirNames = getJournalDirNames();
        File[] journalDirs = new File[journalDirNames.length];
        for (int i = 0; i < journalDirNames.length; i++) {
            journalDirs[i] = new File(journalDirNames[i]);
        }
        return journalDirs;
    }

    /**
     * Get dir names to store ledger data.
     *
     * @return ledger dir names, if not provided return null
     */
    public String[] getLedgerDirWithoutDefault() {
        return LEDGER_DIRS_KEY.getArrayWithoutDefault(this);
    }

    /**
     * Get dir names to store ledger data.
     *
     * @return ledger dir names, if not provided return null
     */
    public String[] getLedgerDirNames() {
        return LEDGER_DIRS_KEY.getArray(this);
    }

    /**
     * Set dir names to store ledger data.
     *
     * @param ledgerDirs
     *          Dir names to store ledger data
     * @return server configuration
     */
    public ServerConfiguration setLedgerDirNames(String[] ledgerDirs) {
        LEDGER_DIRS_KEY.set(this, ledgerDirs);
        return this;
    }

    /**
     * Get dirs that stores ledger data.
     *
     * @return ledger dirs
     */
    public File[] getLedgerDirs() {
        String[] ledgerDirNames = getLedgerDirNames();

        File[] ledgerDirs = new File[ledgerDirNames.length];
        for (int i = 0; i < ledgerDirNames.length; i++) {
            ledgerDirs[i] = new File(ledgerDirNames[i]);
        }
        return ledgerDirs;
    }

    /**
     * Get dir name to store index files.
     *
     * @return ledger index dir name, if no index dirs provided return null
     */
    public String[] getIndexDirNames() {
        return INDEX_DIRS_KEY.getArrayWithoutDefault(this);
    }

    /**
     * Set dir name to store index files.
     *
     * @param indexDirs
     *          Index dir names
     * @return server configuration.
     */
    public ServerConfiguration setIndexDirName(String[] indexDirs) {
        INDEX_DIRS_KEY.set(this, indexDirs);
        return this;
    }

    /**
     * Get index dir to store ledger index files.
     *
     * @return index dirs, if no index dirs provided return null
     */
    public File[] getIndexDirs() {
        String[] idxDirNames = getIndexDirNames();
        if (null == idxDirNames) {
            return null;
        }
        File[] idxDirs = new File[idxDirNames.length];
        for (int i = 0; i < idxDirNames.length; i++) {
            idxDirs[i] = new File(idxDirNames[i]);
        }
        return idxDirs;
    }

    /**
     * Is tcp connection no delay.
     *
     * @return tcp socket nodelay setting
     */
    public boolean getServerTcpNoDelay() {
        return SERVER_TCP_NODELAY_KEY.getBoolean(this);
    }

    /**
     * Set socket nodelay setting.
     *
     * @param noDelay
     *          NoDelay setting
     * @return server configuration
     */
    public ServerConfiguration setServerTcpNoDelay(boolean noDelay) {
        SERVER_TCP_NODELAY_KEY.set(this, noDelay);
        return this;
    }

    /**
     * Get the number of IO threads. This is the number of
     * threads used by Netty to handle TCP connections.
     *
     * @return the number of IO threads
     */
    public int getServerNumIOThreads() {
        return SERVER_NUM_IO_THREADS_KEY.getInt(this);
    }

    /**
     * Set the number of IO threads.
     *
     * <p>
     * This is the number of threads used by Netty to handle TCP connections.
     * </p>
     *
     * @see #getNumIOThreads()
     * @param numThreads number of IO threads used for bookkeeper
     * @return client configuration
     */
    public ServerConfiguration setServerNumIOThreads(int numThreads) {
        SERVER_NUM_IO_THREADS_KEY.set(this, numThreads);
        return this;
    }

    /**
     * Timeout to drain the socket on close.
     *
     * @return socket linger setting
     */
    public int getServerSockLinger() {
        return SERVER_SOCK_LINGER_KEY.getInt(this);
    }

    /**
     * Set socket linger timeout on close.
     *
     * <p>When enabled, a close or shutdown will not return until all queued messages for the socket have been
     * successfully sent or the linger timeout has been reached. Otherwise, the call returns immediately and the
     * closing is done in the background.
     *
     * @param linger
     *            NoDelay setting
     * @return server configuration
     */
    public ServerConfiguration setServerSockLinger(int linger) {
        SERVER_SOCK_LINGER_KEY.set(this, linger);
        return this;
    }

    /**
     * Get socket keepalive.
     *
     * @return socket keepalive setting
     */
    public boolean getServerSockKeepalive() {
        return SERVER_SOCK_KEEPALIVE_KEY.getBoolean(this);
    }

    /**
     * Set socket keepalive setting.
     *
     * <p>This setting is used to send keep-alive messages on connection-oriented sockets.
     *
     * @param keepalive
     *            KeepAlive setting
     * @return server configuration
     */
    public ServerConfiguration setServerSockKeepalive(boolean keepalive) {
        SERVER_SOCK_KEEPALIVE_KEY.set(this, keepalive);
        return this;
    }

    /**
     * Get zookeeper client backoff retry start time in millis.
     *
     * @return zk backoff retry start time in millis.
     */
    public int getZkRetryBackoffStartMs() {
        return ZK_RETRY_BACKOFF_START_MS_KEY.getInt(this);
    }

    /**
     * Set zookeeper client backoff retry start time in millis.
     *
     * @param retryMs
     *          backoff retry start time in millis.
     * @return server configuration.
     */
    public ServerConfiguration setZkRetryBackoffStartMs(int retryMs) {
        ZK_RETRY_BACKOFF_START_MS_KEY.set(this, retryMs);
        return this;
    }

    /**
     * Get zookeeper client backoff retry max time in millis.
     *
     * @return zk backoff retry max time in millis.
     */
    public int getZkRetryBackoffMaxMs() {
        return ZK_RETRY_BACKOFF_MAX_MS_KEY.getInt(this);
    }

    /**
     * Set zookeeper client backoff retry max time in millis.
     *
     * @param retryMs
     *          backoff retry max time in millis.
     * @return server configuration.
     */
    public ServerConfiguration setZkRetryBackoffMaxMs(int retryMs) {
        ZK_RETRY_BACKOFF_MAX_MS_KEY.set(this, retryMs);
        return this;
    }

    /**
     * Is statistics enabled.
     *
     * @return is statistics enabled
     */
    public boolean isStatisticsEnabled() {
        return ENABLE_STATISTICS_KEY.getBoolean(this);
    }

    /**
     * Turn on/off statistics.
     *
     * @param enabled
     *          Whether statistics enabled or not.
     * @return server configuration
     */
    public ServerConfiguration setStatisticsEnabled(boolean enabled) {
        ENABLE_STATISTICS_KEY.set(this, enabled);
        return this;
    }

    /**
     * Get threshold of minor compaction.
     *
     * <p>For those entry log files whose remaining size percentage reaches below
     * this threshold  will be compacted in a minor compaction.
     *
     * <p>If it is set to less than zero, the minor compaction is disabled.
     *
     * @return threshold of minor compaction
     */
    public double getMinorCompactionThreshold() {
        return MINOR_COMPACTION_THRESHOLD_KEY.getDouble(this);
    }

    /**
     * Set threshold of minor compaction.
     *
     * @see #getMinorCompactionThreshold()
     *
     * @param threshold
     *          Threshold for minor compaction
     * @return server configuration
     */
    public ServerConfiguration setMinorCompactionThreshold(double threshold) {
        MINOR_COMPACTION_THRESHOLD_KEY.set(this, threshold);
        return this;
    }

    /**
     * Get threshold of major compaction.
     *
     * <p>For those entry log files whose remaining size percentage reaches below
     * this threshold  will be compacted in a major compaction.
     *
     * <p>If it is set to less than zero, the major compaction is disabled.
     *
     * @return threshold of major compaction
     */
    public double getMajorCompactionThreshold() {
        return MAJOR_COMPACTION_THRESHOLD_KEY.getDouble(this);
    }

    /**
     * Set threshold of major compaction.
     *
     * @see #getMajorCompactionThreshold()
     *
     * @param threshold
     *          Threshold of major compaction
     * @return server configuration
     */
    public ServerConfiguration setMajorCompactionThreshold(double threshold) {
        MAJOR_COMPACTION_THRESHOLD_KEY.set(this, threshold);
        return this;
    }

    /**
     * Get interval to run minor compaction, in seconds.
     *
     * <p>If it is set to less than zero, the minor compaction is disabled.
     *
     * @return threshold of minor compaction
     */
    public long getMinorCompactionInterval() {
        return MINOR_COMPACTION_INTERVAL_KEY.getLong(this);
    }

    /**
     * Set interval to run minor compaction.
     *
     * @see #getMinorCompactionInterval()
     *
     * @param interval
     *          Interval to run minor compaction
     * @return server configuration
     */
    public ServerConfiguration setMinorCompactionInterval(long interval) {
        MINOR_COMPACTION_INTERVAL_KEY.set(this, interval);
        return this;
    }

    /**
     * Get interval to run major compaction, in seconds.
     *
     * <p>If it is set to less than zero, the major compaction is disabled.
     *
     * @return high water mark
     */
    public long getMajorCompactionInterval() {
        return MAJOR_COMPACTION_INTERVAL_KEY.getLong(this);
    }

    /**
     * Set interval to run major compaction.
     *
     * @see #getMajorCompactionInterval()
     *
     * @param interval
     *          Interval to run major compaction
     * @return server configuration
     */
    public ServerConfiguration setMajorCompactionInterval(long interval) {
        MAJOR_COMPACTION_INTERVAL_KEY.set(this, interval);
        return this;
    }

    /**
     * Get whether force compaction is allowed when disk full or almost full.
     *
     * <p>Force GC may get some space back, but may also fill up disk space more
     * quickly. This is because new log files are created before GC, while old
     * garbage log files deleted after GC.
     *
     * @return true  - do force GC when disk full,
     *         false - suspend GC when disk full.
     */
    public boolean getIsForceGCAllowWhenNoSpace() {
        return IS_FORCE_GC_ALLOW_WHEN_NO_SPACE_KEY.getBoolean(this);
    }

    /**
     * Set whether force GC is allowed when disk full or almost full.
     *
     * @param force true to allow force GC; false to suspend GC
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setIsForceGCAllowWhenNoSpace(boolean force) {
        IS_FORCE_GC_ALLOW_WHEN_NO_SPACE_KEY.set(this, force);
        return this;
    }

    /**
     * Set the grace period which the rereplication worker will wait before
     * fencing and rereplicating a ledger fragment which is still being written
     * to, on bookie failure.
     *
     * <p>The grace period allows the writer to detect the bookie failure, and and
     * start writing to another ledger fragment. If the writer writes nothing
     * during the grace period, the rereplication worker assumes that it has
     * crashed and therefore fences the ledger, preventing any further writes to
     * that ledger.
     *
     * @see org.apache.bookkeeper.client.BookKeeper#openLedger
     *
     * @param waitTime time to wait before replicating ledger fragment
     */
    public void setOpenLedgerRereplicationGracePeriod(String waitTime) {
        OPEN_LEDGER_REREPLICATION_GRACE_PERIOD_KEY.set(this, waitTime);
    }

    /**
     * Get the grace period which the rereplication worker to wait before
     * fencing and rereplicating a ledger fragment which is still being written
     * to, on bookie failure.
     *
     * @return long
     */
    public long getOpenLedgerRereplicationGracePeriod() {
        return OPEN_LEDGER_REREPLICATION_GRACE_PERIOD_KEY.getLong(this);
    }

    /**
     * Set the grace period so that if the replication worker fails to replicate
     * a underreplicatedledger for more than
     * ReplicationWorker.MAXNUMBER_REPLICATION_FAILURES_ALLOWED_BEFORE_DEFERRING
     * number of times, then instead of releasing the lock immediately after
     * failed attempt, it will hold under replicated ledger lock for this grace
     * period and then it will release the lock.
     *
     * @param waitTime
     */
    public void setLockReleaseOfFailedLedgerGracePeriod(long waitTime) {
        LOCK_RELEASE_OF_FAILED_LEDGER_GRACE_PERIOD_KEY.set(this, waitTime);
    }

    /**
     * Get the grace period which the replication worker to wait before
     * releasing the lock after replication worker failing to replicate for more
     * than
     * ReplicationWorker.MAXNUMBER_REPLICATION_FAILURES_ALLOWED_BEFORE_DEFERRING
     * number of times.
     *
     * @return
     */
    public long getLockReleaseOfFailedLedgerGracePeriod() {
        return LOCK_RELEASE_OF_FAILED_LEDGER_GRACE_PERIOD_KEY.getLong(this);
    }

    /**
     * Get the number of bytes we should use as capacity for
     * org.apache.bookkeeper.bookie.BufferedReadChannel.
     * Default is 512 bytes
     * @return read buffer size
     */
    public int getReadBufferBytes() {
        return READ_BUFFER_SIZE_KEY.getInt(this);
    }

    /**
     * Set the number of bytes we should use as capacity for
     * org.apache.bookkeeper.bookie.BufferedReadChannel.
     *
     * @param readBufferSize
     *          Read Buffer Size
     * @return server configuration
     */
    public ServerConfiguration setReadBufferBytes(int readBufferSize) {
        READ_BUFFER_SIZE_KEY.set(this, readBufferSize);
        return this;
    }

    /**
     * Set the number of threads that would handle write requests.
     *
     * @param numThreads
     *          number of threads to handle write requests.
     * @return server configuration
     */
    public ServerConfiguration setNumAddWorkerThreads(int numThreads) {
        NUM_ADD_WORKER_THREADS_KEY.set(this, numThreads);
        return this;
    }

    /**
     * Get the number of threads that should handle write requests.
     *
     * @return the number of threads that handle write requests.
     */
    public int getNumAddWorkerThreads() {
        return NUM_ADD_WORKER_THREADS_KEY.getInt(this);
    }

    /**
     * Set the number of threads that should handle long poll requests.
     *
     * @param numThreads
     *          number of threads to handle long poll requests.
     * @return server configuration
     */
    public ServerConfiguration setNumLongPollWorkerThreads(int numThreads) {
        NUM_LONG_POLL_WORKER_THREADS_KEY.set(this, numThreads);
        return this;
    }

    /**
     * Get the number of threads that should handle long poll requests.
     *
     * <p>If the number of threads is zero or negative, bookie will fallback to
     * use read threads. If there is no read threads used, it will create a thread pool
     * with {@link Runtime#availableProcessors()} threads.
     *
     * @return the number of threads that should handle long poll requests, default value is 0.
     */
    public int getNumLongPollWorkerThreads() {
        return NUM_LONG_POLL_WORKER_THREADS_KEY.getInt(this);
    }

    /**
     * Set the number of threads that should be used for high priority requests
     * (i.e. recovery reads and adds, and fencing)
     *
     * @param numThreads
     *          number of threads to handle high priority requests.
     * @return server configuration
     */
    public ServerConfiguration setNumHighPriorityWorkerThreads(int numThreads) {
        NUM_HIGH_PRIORITY_WORKER_THREADS_KEY.set(this, numThreads);
        return this;
    }

    /**
     * Get the number of threads that should be used for high priority requests
     * (i.e. recovery reads and adds, and fencing)
     * @return
     */
    public int getNumHighPriorityWorkerThreads() {
        return NUM_HIGH_PRIORITY_WORKER_THREADS_KEY.getInt(this);
    }


    /**
     * Set the number of threads that would handle read requests.
     *
     * @param numThreads
     *          Number of threads to handle read requests.
     * @return server configuration
     */
    public ServerConfiguration setNumReadWorkerThreads(int numThreads) {
        NUM_READ_WORKER_THREADS_KEY.set(this, numThreads);
        return this;
    }

    /**
     * Get the number of threads that should handle read requests.
     */
    public int getNumReadWorkerThreads() {
        return NUM_READ_WORKER_THREADS_KEY.getInt(this);
    }

    /**
     * Set the tick duration in milliseconds.
     *
     * @param tickDuration
     *          tick duration in milliseconds.
     * @return server configuration
     */
    public ServerConfiguration setRequestTimerTickDurationMs(int tickDuration) {
        REQUEST_TIMER_TICK_DURATION_MILLISEC_KEY.set(this, tickDuration);
        return this;
    }

    /**
     * Set the max number of pending read requests for each read worker thread. After the quota is reached,
     * new requests will be failed immediately.
     *
     * @param maxPendingReadRequestsPerThread
     * @return server configuration
     */
    public ServerConfiguration setMaxPendingReadRequestPerThread(int maxPendingReadRequestsPerThread) {
        MAX_PENDING_READ_REQUESTS_PER_THREAD_KEY.set(this, maxPendingReadRequestsPerThread);
        return this;
    }

    /**
     * If read workers threads are enabled, limit the number of pending requests, to avoid the executor queue to grow
     * indefinitely (default: 10000 entries).
     */
    public int getMaxPendingReadRequestPerThread() {
        return MAX_PENDING_READ_REQUESTS_PER_THREAD_KEY.getInt(this);
    }

    /**
     * Set the max number of pending add requests for each add worker thread. After the quota is reached, new requests
     * will be failed immediately.
     *
     * @param maxPendingAddRequestsPerThread
     * @return server configuration
     */
    public ServerConfiguration setMaxPendingAddRequestPerThread(int maxPendingAddRequestsPerThread) {
        MAX_PENDING_ADD_REQUESTS_PER_THREAD_KEY.set(this, maxPendingAddRequestsPerThread);
        return this;
    }

    /**
     * If add workers threads are enabled, limit the number of pending requests, to avoid the executor queue to grow
     * indefinitely (default: 10000 entries).
     */
    public int getMaxPendingAddRequestPerThread() {
        return MAX_PENDING_ADD_REQUESTS_PER_THREAD_KEY.getInt(this);
    }

    /**
     * Get the tick duration in milliseconds.
     * @return
     */
    public int getRequestTimerTickDurationMs() {
        return REQUEST_TIMER_TICK_DURATION_MILLISEC_KEY.getInt(this);
    }

    /**
     * Set the number of ticks per wheel for the request timer.
     *
     * @param tickCount
     *          number of ticks per wheel for the request timer.
     * @return server configuration
     */
    public ServerConfiguration setRequestTimerNumTicks(int tickCount) {
        REQUEST_TIMER_NO_OF_TICKS_KEY.set(this, tickCount);
        return this;
    }

    /**
     * Get the number of ticks per wheel for the request timer.
     * @return
     */
    public int getRequestTimerNumTicks() {
        return REQUEST_TIMER_NO_OF_TICKS_KEY.getInt(this);
    }

    /**
     * Get the number of bytes used as capacity for the write buffer. Default is
     * 64KB.
     * NOTE: Make sure this value is greater than the maximum message size.
     * @return the size of the write buffer in bytes
     */
    public int getWriteBufferBytes() {
        return WRITE_BUFFER_SIZE_KEY.getInt(this);
    }

    /**
     * Set the number of bytes used as capacity for the write buffer.
     *
     * @param writeBufferBytes
     *          Write Buffer Bytes
     * @return server configuration
     */
    public ServerConfiguration setWriteBufferBytes(int writeBufferBytes) {
        WRITE_BUFFER_SIZE_KEY.set(this, writeBufferBytes);
        return this;
    }

    /**
     * Set the number of threads that would handle journal callbacks.
     *
     * @param numThreads
     *          number of threads to handle journal callbacks.
     * @return server configuration
     */
    public ServerConfiguration setNumJournalCallbackThreads(int numThreads) {
        NUM_JOURNAL_CALLBACK_THREADS_KEY.set(this, numThreads);
        return this;
    }

    /**
     * Get the number of threads that should handle journal callbacks.
     *
     * @return the number of threads that handle journal callbacks.
     */
    public int getNumJournalCallbackThreads() {
        return NUM_JOURNAL_CALLBACK_THREADS_KEY.getInt(this);
    }

    /**
     * Set sorted-ledger storage enabled or not.
     *
     * @deprecated Use {@link #setLedgerStorageClass(String)} to configure the implementation class
     * @param enabled
     */
    public ServerConfiguration setSortedLedgerStorageEnabled(boolean enabled) {
        SORTED_LEDGER_STORAGE_ENABLED_KEY.set(this, enabled);
        return this;
    }

    /**
     * Check if sorted-ledger storage enabled (default true).
     *
     * @return true if sorted ledger storage is enabled, false otherwise
     */
    public boolean getSortedLedgerStorageEnabled() {
        return SORTED_LEDGER_STORAGE_ENABLED_KEY.getBoolean(this);
    }

    /**
     * Get skip list data size limitation (default 64MB).
     * Max value is 1,073,741,823
     *
     * @return skip list data size limitation
     */
    public long getSkipListSizeLimit() {
        return SKIP_LIST_SIZE_LIMIT_KEY.getLong(this);
    }

    /**
     * Set skip list size limit.
     *
     * @param size skip list size limit.
     * @return server configuration object.
     */
    public ServerConfiguration setSkipListSizeLimit(int size) {
        SKIP_LIST_SIZE_LIMIT_KEY.set(this, size);
        return this;
    }

    /**
     * Get the number of bytes we should use as chunk allocation for
     * org.apache.bookkeeper.bookie.SkipListArena.
     * Default is 4 MB
     * @return the number of bytes to use for each chunk in the skiplist arena
     */
    public int getSkipListArenaChunkSize() {
        return SKIP_LIST_CHUNK_SIZE_ENTRY_KEY.getInt(this);
    }

    /**
     * Set the number of bytes we used as chunk allocation for
     * org.apache.bookkeeper.bookie.SkipListArena.
     *
     * @param size chunk size.
     * @return server configuration object.
     */
    public ServerConfiguration setSkipListArenaChunkSize(int size) {
        SKIP_LIST_CHUNK_SIZE_ENTRY_KEY.set(this, size);
        return this;
    }

    /**
     * Get the max size we should allocate from the skiplist arena. Allocations
     * larger than this should be allocated directly by the VM to avoid fragmentation.
     *
     * @return max size allocatable from the skiplist arena (Default is 128 KB)
     */
    public int getSkipListArenaMaxAllocSize() {
        return SKIP_LIST_MAX_ALLOC_ENTRY_KEY.getInt(this);
    }

    /**
     * Set the max size we should allocate from the skiplist arena. Allocations
     * larger than this should be allocated directly by the VM to avoid fragmentation.
     *
     * @param size max alloc size.
     * @return server configuration object.
     */
    public ServerConfiguration setSkipListArenaMaxAllocSize(int size) {
        SKIP_LIST_MAX_ALLOC_ENTRY_KEY.set(this, size);
        return this;
    }

    /**
     * Should the data be fsynced on journal before acknowledgment.
     *
     * <p>Default is true
     *
     * @return
     */
    public boolean getJournalSyncData() {
        return JOURNAL_SYNC_DATA_KEY.getBoolean(this);
    }

    /**
     * Enable or disable journal syncs.
     *
     * <p>By default, data sync is enabled to guarantee durability of writes.
     *
     * <p>Beware: while disabling data sync in the Bookie journal might improve the bookie write performance, it will
     * also introduce the possibility of data loss. With no sync, the journal entries are written in the OS page cache
     * but not flushed to disk. In case of power failure, the affected bookie might lose the unflushed data. If the
     * ledger is replicated to multiple bookies, the chances of data loss are reduced though still present.
     *
     * @param syncData
     *            whether to sync data on disk before acknowledgement
     * @return server configuration object
     */
    public ServerConfiguration setJournalSyncData(boolean syncData) {
        JOURNAL_SYNC_DATA_KEY.set(this, syncData);
        return this;
    }

    /**
     * Should we group journal force writes.
     *
     * @return group journal force writes
     */
    public boolean getJournalAdaptiveGroupWrites() {
        return JOURNAL_ADAPTIVE_GROUP_WRITES_KEY.getBoolean(this);
    }

    /**
     * Enable/disable group journal force writes.
     *
     * @param enabled flag to enable/disable group journal force writes
     */
    public ServerConfiguration setJournalAdaptiveGroupWrites(boolean enabled) {
        JOURNAL_ADAPTIVE_GROUP_WRITES_KEY.set(this, enabled);
        return this;
    }

    /**
     * Maximum latency to impose on a journal write to achieve grouping. Default is 2ms.
     *
     * @return max wait for grouping
     */
    public long getJournalMaxGroupWaitMSec() {
        return JOURNAL_MAX_GROUP_WAIT_MSEC_KEY.getLong(this);
    }

    /**
     * Sets the maximum latency to impose on a journal write to achieve grouping.
     *
     * @param journalMaxGroupWaitMSec
     *          maximum time to wait in milliseconds.
     * @return server configuration.
     */
    public ServerConfiguration setJournalMaxGroupWaitMSec(long journalMaxGroupWaitMSec) {
        JOURNAL_MAX_GROUP_WAIT_MSEC_KEY.set(this, journalMaxGroupWaitMSec);
        return this;
    }

    /**
     * Maximum bytes to buffer to impose on a journal write to achieve grouping.
     *
     * @return max bytes to buffer
     */
    public long getJournalBufferedWritesThreshold() {
        return JOURNAL_BUFFERED_WRITES_THRESHOLD_KEY.getLong(this);
    }

    /**
     * Maximum entries to buffer to impose on a journal write to achieve grouping.
     * Use {@link #getJournalBufferedWritesThreshold()} if this is set to zero or
     * less than zero.
     *
     * @return max entries to buffer.
     */
    public long getJournalBufferedEntriesThreshold() {
        return JOURNAL_BUFFERED_ENTRIES_THRESHOLD_KEY.getLong(this);
    }

    /**
     * Set maximum entries to buffer to impose on a journal write to achieve grouping.
     * Use {@link #getJournalBufferedWritesThreshold()} set this to zero or less than
     * zero.
     *
     * @param maxEntries
     *          maximum entries to buffer.
     * @return server configuration.
     */
    public ServerConfiguration setJournalBufferedEntriesThreshold(int maxEntries) {
        setProperty(JOURNAL_BUFFERED_ENTRIES_THRESHOLD, maxEntries);
        return this;
    }

    /**
     * Set if we should flush the journal when queue is empty.
     */
    public ServerConfiguration setJournalFlushWhenQueueEmpty(boolean enabled) {
        JOURNAL_FLUSH_WHEN_QUEUE_EMPTY_KEY.set(this, enabled);
        return this;
    }

    /**
     * Should we flush the journal when queue is empty.
     *
     * @return flush when queue is empty
     */
    public boolean getJournalFlushWhenQueueEmpty() {
        return JOURNAL_FLUSH_WHEN_QUEUE_EMPTY_KEY.getBoolean(this);
    }

    /**
     * Set whether the bookie is able to go into read-only mode.
     * If this is set to false, the bookie will shutdown on encountering
     * an error condition.
     *
     * @param enabled whether to enable read-only mode.
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setReadOnlyModeEnabled(boolean enabled) {
        READ_ONLY_MODE_ENABLED_KEY.set(this, enabled);
        return this;
    }

    /**
     * Get whether read-only mode is enabled. The default is true.
     *
     * @return boolean
     */
    public boolean isReadOnlyModeEnabled() {
        return READ_ONLY_MODE_ENABLED_KEY.getBoolean(this);
    }

    /**
     * Set the warning threshold for disk usage.
     *
     * @param threshold warning threshold to force gc.
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setDiskUsageWarnThreshold(float threshold) {
        DISK_USAGE_WARN_THRESHOLD_KEY.set(this, threshold);
        return this;
    }

    /**
     * Returns the warning threshold for disk usage. If disk usage
     * goes beyond this, a garbage collection cycle will be forced.
     * @return the percentage at which a disk usage warning will trigger
     */
    public float getDiskUsageWarnThreshold() {
        return DISK_USAGE_WARN_THRESHOLD_KEY.getFloat(this);
    }

    /**
     * Whether to persist the bookie status so that when bookie server restarts,
     * it will continue using the previous status.
     *
     * @param enabled
     *            - true if persist the bookie status. Otherwise false.
     * @return ServerConfiguration
     */
    public ServerConfiguration setPersistBookieStatusEnabled(boolean enabled) {
        PERSIST_BOOKIE_STATUS_ENABLED_KEY.set(this, enabled);
        return this;
    }

    /**
     * Get whether to persist the bookie status so that when bookie server restarts,
     * it will continue using the previous status.
     *
     * @return true - if need to start a bookie in read only mode. Otherwise false.
     */
    public boolean isPersistBookieStatusEnabled() {
        return PERSIST_BOOKIE_STATUS_ENABLED_KEY.getBoolean(this);
    }

    /**
     * Set the Disk free space threshold as a fraction of the total
     * after which disk will be considered as full during disk check.
     *
     * @param threshold threshold to declare a disk full
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setDiskUsageThreshold(float threshold) {
        DISK_USAGE_THRESHOLD_KEY.set(this, threshold);
        return this;
    }

    /**
     * Returns disk free space threshold. By default it is 0.95.
     *
     * @return the percentage at which a disk will be considered full
     */
    public float getDiskUsageThreshold() {
        return DISK_USAGE_THRESHOLD_KEY.getFloat(this);
    }

    /**
     * Set the disk free space low water mark threshold.
     * Disk is considered full when usage threshold is exceeded.
     * Disk returns back to non-full state when usage is below low water mark threshold.
     * This prevents it from going back and forth between these states frequently
     * when concurrent writes and compaction are happening. This also prevent bookie from
     * switching frequently between read-only and read-writes states in the same cases.
     *
     * @param threshold threshold to declare a disk full
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setDiskLowWaterMarkUsageThreshold(float threshold) {
        DISK_USAGE_LWM_THRESHOLD_KEY.set(this, threshold);
        return this;
    }

    /**
     * Returns disk free space low water mark threshold. By default it is the
     * same as usage threshold (for backwards-compatibility).
     *
     * @return the percentage below which a disk will NOT be considered full
     */
    public float getDiskLowWaterMarkUsageThreshold() {
        return DISK_USAGE_LWM_THRESHOLD_KEY.getFloat(this);
    }

    /**
     * Set the disk checker interval to monitor ledger disk space.
     *
     * @param interval interval between disk checks for space.
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setDiskCheckInterval(int interval) {
        DISK_CHECK_INTERVAL_KEY.set(this, interval);
        return this;
    }

    /**
     * Get the disk checker interval.
     *
     * @return int
     */
    public int getDiskCheckInterval() {
        return DISK_CHECK_INTERVAL_KEY.getInt(this);
    }

    /**
     * Set the regularity at which the auditor will run a check
     * of all ledgers. This should not be run very often, and at most,
     * once a day. Setting this to 0 will completely disable the periodic
     * check.
     *
     * @param interval The interval in seconds. e.g. 86400 = 1 day, 604800 = 1 week
     */
    public void setAuditorPeriodicCheckInterval(long interval) {
        AUDITOR_PERIODIC_CHECK_INTERVAL_KEY.set(this, interval);
    }

    /**
     * Get the regularity at which the auditor checks all ledgers.
     * @return The interval in seconds. Default is 604800 (1 week).
     */
    public long getAuditorPeriodicCheckInterval() {
        return AUDITOR_PERIODIC_CHECK_INTERVAL_KEY.getLong(this);
    }

    /**
     * Set the interval between auditor bookie checks.
     * The auditor bookie check, checks ledger metadata to see which bookies
     * contain entries for each ledger. If a bookie which should contain entries
     * is unavailable, then the ledger containing that entry is marked for recovery.
     * Setting this to 0 disabled the periodic check. Bookie checks will still
     * run when a bookie fails.
     *
     * @param interval The period in seconds.
     */
    public void setAuditorPeriodicBookieCheckInterval(long interval) {
        AUDITOR_PERIODIC_BOOKIE_CHECK_INTERVAL_KEY.set(this, interval);
    }

    /**
     * Get the interval between auditor bookie check runs.
     * @see #setAuditorPeriodicBookieCheckInterval(long)
     * @return the interval between bookie check runs, in seconds. Default is 86400 (= 1 day)
     */
    public long getAuditorPeriodicBookieCheckInterval() {
        return AUDITOR_PERIODIC_BOOKIE_CHECK_INTERVAL_KEY.getLong(this);
    }

    /**
     * Set what percentage of a ledger (fragment)'s entries will be verified.
     * 0 - only the first and last entry of each ledger fragment would be verified
     * 100 - the entire ledger fragment would be verified
     * anything else - randomly picked entries from over the fragment would be verifiec
     * @param auditorLedgerVerificationPercentage The verification proportion as a percentage
     * @return ServerConfiguration
     */
    public ServerConfiguration setAuditorLedgerVerificationPercentage(long auditorLedgerVerificationPercentage) {
        AUDITOR_LEDGER_VERIFICATION_PERCENTAGE_KEY.set(this, auditorLedgerVerificationPercentage);
        return this;
    }

    /**
     * Get what percentage of a ledger (fragment)'s entries will be verified.
     * @see #setAuditorLedgerVerificationPercentage(long)
     * @return percentage of a ledger (fragment)'s entries will be verified. Default is 0.
     */
    public long getAuditorLedgerVerificationPercentage() {
        return AUDITOR_LEDGER_VERIFICATION_PERCENTAGE_KEY.getLong(this);
    }

    /**
     * Sets that whether the auto-recovery service can start along with Bookie
     * server itself or not.
     *
     * @param enabled
     *            - true if need to start auto-recovery service. Otherwise
     *            false.
     * @return ServerConfiguration
     */
    public ServerConfiguration setAutoRecoveryDaemonEnabled(boolean enabled) {
        AUTO_RECOVERY_DAEMON_ENABLED_KEY.set(this, enabled);
        return this;
    }

    /**
     * Get whether the Bookie itself can start auto-recovery service also or not.
     *
     * @return true - if Bookie should start auto-recovery service along with
     *         it. false otherwise.
     */
    public boolean isAutoRecoveryDaemonEnabled() {
        return AUTO_RECOVERY_DAEMON_ENABLED_KEY.getBoolean(this);
    }

    /**
     * Get how long to delay the recovery of ledgers of a lost bookie.
     *
     * @return delay interval in seconds
     */
    public int getLostBookieRecoveryDelay() {
        return LOST_BOOKIE_RECOVERY_DELAY_KEY.getInt(this);
    }

    /**
     * Set the delay interval for starting recovery of a lost bookie.
     */
    public void setLostBookieRecoveryDelay(int interval) {
        LOST_BOOKIE_RECOVERY_DELAY_KEY.set(this, interval);
    }

    /**
     * Get how long to backoff when encountering exception on rereplicating a ledger.
     *
     * @return backoff time in milliseconds
     */
    public int getRwRereplicateBackoffMs() {
        return RW_REREPLICATE_BACKOFF_MS_KEY.getInt(this);
    }

    /**
     * Set how long to backoff when encountering exception on rereplicating a ledger.
     *
     * @param backoffMs backoff time in milliseconds
     */
    public void setRwRereplicateBackoffMs(int backoffMs) {
        RW_REREPLICATE_BACKOFF_MS_KEY.set(this, backoffMs);
    }

    /**
     * Sets that whether force start a bookie in readonly mode.
     *
     * @param enabled
     *            - true if need to start a bookie in read only mode. Otherwise
     *            false.
     * @return ServerConfiguration
     */
    public ServerConfiguration setForceReadOnlyBookie(boolean enabled) {
        FORCE_READ_ONLY_BOOKIE_KEY.set(this, enabled);
        return this;
    }

    /**
     * Get whether the Bookie is force started in read only mode or not.
     *
     * @return true - if need to start a bookie in read only mode. Otherwise
     *         false.
     */
    public boolean isForceReadOnlyBookie() {
        return FORCE_READ_ONLY_BOOKIE_KEY.getBoolean(this);
    }

    /**
     * Get whether use bytes to throttle garbage collector compaction or not.
     *
     * @return true  - use Bytes,
     *         false - use Entries.
     */
    public boolean getIsThrottleByBytes() {
        return IS_THROTTLE_BY_BYTES_KEY.getBoolean(this);
    }

    /**
     * Set whether use bytes to throttle garbage collector compaction or not.
     *
     * @param byBytes true to use by bytes; false to use by entries
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setIsThrottleByBytes(boolean byBytes) {
        IS_THROTTLE_BY_BYTES_KEY.set(this, byBytes);
        return this;
    }

    /**
     * Get the maximum number of entries which can be compacted without flushing.
     * Default is 100,000.
     *
     * @return the maximum number of unflushed entries
     */
    public int getCompactionMaxOutstandingRequests() {
        return COMPACTION_MAX_OUTSTANDING_REQUESTS_KEY.getInt(this);
    }

    /**
     * Set the maximum number of entries which can be compacted without flushing.
     *
     * <p>When compacting, the entries are written to the entrylog and the new offsets
     * are cached in memory. Once the entrylog is flushed the index is updated with
     * the new offsets. This parameter controls the number of entries added to the
     * entrylog before a flush is forced. A higher value for this parameter means
     * more memory will be used for offsets. Each offset consists of 3 longs.
     *
     * <p>This parameter should _not_ be modified unless you know what you're doing.
     * The default is 100,000.
     *
     * @param maxOutstandingRequests number of entries to compact before flushing
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setCompactionMaxOutstandingRequests(int maxOutstandingRequests) {
        COMPACTION_MAX_OUTSTANDING_REQUESTS_KEY.set(this, maxOutstandingRequests);
        return this;
    }

    /**
     * Get the rate of compaction adds. Default is 1,000.
     *
     * @return rate of compaction (adds per second)
     * @deprecated  replaced by {@link #getCompactionRateByEntries()}
     */
    @Deprecated
    public int getCompactionRate() {
        return COMPACTION_RATE_KEY.getInt(this);
    }

    /**
     * Set the rate of compaction adds.
     *
     * @param rate rate of compaction adds (adds entries per second)
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setCompactionRate(int rate) {
        COMPACTION_RATE_KEY.set(this, rate);
        return this;
    }

    /**
     * Get the rate of compaction adds. Default is 1,000.
     *
     * @return rate of compaction (adds entries per second)
     */
    public int getCompactionRateByEntries() {
        return COMPACTION_RATE_BY_ENTRIES_KEY.getInt(this);
    }

    /**
     * Set the rate of compaction adds.
     *
     * @param rate rate of compaction adds (adds entries per second)
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setCompactionRateByEntries(int rate) {
        COMPACTION_RATE_BY_ENTRIES_KEY.set(this, rate);
        return this;
    }

    /**
     * Get the rate of compaction adds. Default is 1,000,000.
     *
     * @return rate of compaction (adds bytes per second)
     */
    public int getCompactionRateByBytes() {
        return COMPACTION_RATE_BY_BYTES_KEY.getInt(this);
    }

    /**
     * Set the rate of compaction adds.
     *
     * @param rate rate of compaction adds (adds bytes per second)
     *
     * @return ServerConfiguration
     */
    public ServerConfiguration setCompactionRateByBytes(int rate) {
        COMPACTION_RATE_BY_BYTES_KEY.set(this, rate);
        return this;
    }

    /**
     * Should we remove pages from page cache after force write.
     *
     * @return remove pages from cache
     */
    @Beta
    public boolean getJournalRemovePagesFromCache() {
        return JOURNAL_REMOVE_FROM_PAGE_CACHE_KEY.getBoolean(this);
    }

    /**
     * Sets that whether should we remove pages from page cache after force write.
     *
     * @param enabled
     *            - true if we need to remove pages from page cache. otherwise, false
     * @return ServerConfiguration
     */
    public ServerConfiguration setJournalRemovePagesFromCache(boolean enabled) {
        JOURNAL_REMOVE_FROM_PAGE_CACHE_KEY.set(this, enabled);
        return this;
    }

    /*
     * Get the {@link LedgerStorage} implementation class name.
     *
     * @return the class name
     */
    public String getLedgerStorageClass() {
        String ledgerStorageClass = LEDGER_STORAGE_CLASS_KEY.getString(this);
        if (ledgerStorageClass.equals(SortedLedgerStorage.class.getName())
                && !getSortedLedgerStorageEnabled()) {
            // This is to retain compatibility with BK-4.3 configuration
            // In BK-4.3, the ledger storage is configured through the "sortedLedgerStorageEnabled" flag :
            // sortedLedgerStorageEnabled == true (default) ---> use SortedLedgerStorage
            // sortedLedgerStorageEnabled == false ---> use InterleavedLedgerStorage
            //
            // Since BK-4.4, one can specify the implementation class, but if it was using InterleavedLedgerStorage it
            // should continue to use that with the same configuration
            ledgerStorageClass = InterleavedLedgerStorage.class.getName();
        }

        return ledgerStorageClass;
    }

    /**
     * Set the {@link LedgerStorage} implementation class name.
     *
     * @param ledgerStorageClass the class name
     * @return ServerConfiguration
     */
    public ServerConfiguration setLedgerStorageClass(String ledgerStorageClass) {
        LEDGER_STORAGE_CLASS_KEY.set(this, ledgerStorageClass);
        return this;
    }

    /**
     * Get whether bookie is using hostname for registration and in ledger
     * metadata. Defaults to false.
     *
     * @return true, then bookie will be registered with its hostname and
     *         hostname will be used in ledger metadata. Otherwise bookie will
     *         use its ipaddress
     */
    public boolean getUseHostNameAsBookieID() {
        return USE_HOST_NAME_AS_BOOKIE_ID_KEY.getBoolean(this);
    }

    /**
     * Configure the bookie to use its hostname to register with the
     * co-ordination service(eg: zookeeper) and in ledger metadata.
     *
     * @see #getUseHostNameAsBookieID
     * @param useHostName
     *            whether to use hostname for registration and in ledgermetadata
     * @return server configuration
     */
    public ServerConfiguration setUseHostNameAsBookieID(boolean useHostName) {
        USE_HOST_NAME_AS_BOOKIE_ID_KEY.set(this, useHostName);
        return this;
    }

    /**
     * If bookie is using hostname for registration and in ledger metadata then
     * whether to use short hostname or FQDN hostname. Defaults to false.
     *
     * @return true, then bookie will be registered with its short hostname and
     *         short hostname will be used in ledger metadata. Otherwise bookie
     *         will use its FQDN hostname
     */
    public boolean getUseShortHostName() {
        return USE_SHORT_HOST_NAME_KEY.getBoolean(this);
    }

    /**
     * Configure the bookie to use its short hostname or FQDN hostname to
     * register with the co-ordination service(eg: zookeeper) and in ledger
     * metadata.
     *
     * @see #getUseShortHostName
     * @param useShortHostName
     *            whether to use short hostname for registration and in
     *            ledgermetadata
     * @return server configuration
     */
    public ServerConfiguration setUseShortHostName(boolean useShortHostName) {
        USE_SHORT_HOST_NAME_KEY.set(this, useShortHostName);
        return this;
    }

    /**
     * Get whether to listen for local JVM clients. Defaults to false.
     *
     * @return true, then bookie will be listen for local JVM clients
     */
    public boolean isEnableLocalTransport() {
        return ENABLE_LOCAL_TRANSPORT_KEY.getBoolean(this);
    }

    /**
     * Configure the bookie to listen for BookKeeper clients executed on the local JVM.
     *
     * @see #isEnableLocalTransport
     * @param enableLocalTransport
     *            whether to use listen for local JVM clients
     * @return server configuration
     */
    public ServerConfiguration setEnableLocalTransport(boolean enableLocalTransport) {
        ENABLE_LOCAL_TRANSPORT_KEY.set(this, enableLocalTransport);
        return this;
    }

    /**
     * Get whether to disable bind of server-side sockets. Defaults to false.
     *
     * @return true, then bookie will not listen for network connections
     */
    public boolean isDisableServerSocketBind() {
        return DISABLE_SERVER_SOCKET_BIND_KEY.getBoolean(this);
    }

    /**
     * Configure the bookie to disable bind on network interfaces,
     * this bookie will be available only to BookKeeper clients executed on the local JVM.
     *
     * @see #isDisableServerSocketBind
     * @param disableServerSocketBind
     *            whether to disable binding on network interfaces
     * @return server configuration
     */
    public ServerConfiguration setDisableServerSocketBind(boolean disableServerSocketBind) {
        DISABLE_SERVER_SOCKET_BIND_KEY.set(this, disableServerSocketBind);
        return this;
    }

    /**
     * Get the stats provider used by bookie.
     *
     * @return stats provider class
     * @throws ConfigurationException
     */
    public Class<? extends StatsProvider> getStatsProviderClass() {
        return STATS_PROVIDER_CLASS_KEY.getClass(this, StatsProvider.class);
    }

    /**
     * Set the stats provider used by bookie.
     *
     * @param providerClass
     *          stats provider class
     * @return server configuration
     */
    public ServerConfiguration setStatsProviderClass(Class<? extends StatsProvider> providerClass) {
        STATS_PROVIDER_CLASS_KEY.set(this, providerClass);
        return this;
    }

    /**
     * Validate the configuration.
     * @throws ConfigurationException
     */
    public void validate() throws ConfigurationException {
        // generate config def
        ConfigDef configDef = ConfigDef.of(ServerConfiguration.class);
        try {
            configDef.validate(this);
        } catch (ConfigException e) {
            throw new ConfigurationException(e.getMessage(), e.getCause());
        }

        if (getSkipListArenaChunkSize() < getSkipListArenaMaxAllocSize()) {
            throw new ConfigurationException("Arena max allocation size should be smaller than the chunk size.");
        }
        if (getJournalAlignmentSize() < 512 || getJournalAlignmentSize() % 512 != 0) {
            throw new ConfigurationException("Invalid journal alignment size : " + getJournalAlignmentSize());
        }
        if (getJournalAlignmentSize() > getJournalPreAllocSizeMB() * 1024 * 1024) {
            throw new ConfigurationException("Invalid preallocation size : " + getJournalPreAllocSizeMB() + " MB");
        }
        if (0 == getBookiePort() && !getAllowEphemeralPorts()) {
            throw new ConfigurationException("Invalid port specified, using ephemeral ports accidentally?");
        }
        if (isEntryLogPerLedgerEnabled() && getUseTransactionalCompaction()) {
            throw new ConfigurationException(
                    "When entryLogPerLedger is enabled , it is unnecessary to use transactional compaction");
        }
        if ((getJournalFormatVersionToWrite() >= 6) ^ (getFileInfoFormatVersionToWrite() >= 1)) {
            throw new ConfigurationException("For persisiting explicitLac, journalFormatVersionToWrite should be >= 6"
                    + "and FileInfoFormatVersionToWrite should be >= 1");
        }
    }

    /**
     * Get Recv ByteBuf allocator initial buf size.
     *
     * @return initial byteBuf size
     */
    public int getRecvByteBufAllocatorSizeInitial() {
        return BYTEBUF_ALLOCATOR_SIZE_INITIAL_KEY.getInt(this);
    }

    /**
     * Set Recv ByteBuf allocator initial buf size.
     *
     * @param size
     *            buffer size
     */
    public void setRecvByteBufAllocatorSizeInitial(int size) {
        BYTEBUF_ALLOCATOR_SIZE_INITIAL_KEY.set(this, size);
    }

    /**
     * Get Recv ByteBuf allocator min buf size.
     *
     * @return min byteBuf size
     */
    public int getRecvByteBufAllocatorSizeMin() {
        return BYTEBUF_ALLOCATOR_SIZE_MIN_KEY.getInt(this);
    }

    /**
     * Set Recv ByteBuf allocator min buf size.
     *
     * @param size
     *            buffer size
     */
    public void setRecvByteBufAllocatorSizeMin(int size) {
        BYTEBUF_ALLOCATOR_SIZE_MIN_KEY.set(this, size);
    }

    /**
     * Get Recv ByteBuf allocator max buf size.
     *
     * @return max byteBuf size
     */
    public int getRecvByteBufAllocatorSizeMax() {
        return BYTEBUF_ALLOCATOR_SIZE_MAX_KEY.getInt(this);
    }

    /**
     * Set Recv ByteBuf allocator max buf size.
     *
     * @param size
     *            buffer size
     */
    public void setRecvByteBufAllocatorSizeMax(int size) {
        BYTEBUF_ALLOCATOR_SIZE_MAX_KEY.set(this, size);
    }

    /**
     * Set the bookie authentication provider factory class name.
     * If this is not set, no authentication will be used.
     *
     * @param factoryClass
     *          the bookie authentication provider factory class name
     */
    public void setBookieAuthProviderFactoryClass(String factoryClass) {
        BOOKIE_AUTH_PROVIDER_FACTORY_CLASS_KEY.set(this, factoryClass);
    }

    /**
     * Get the bookie authentication provider factory class name.
     * If this returns null, no authentication will take place.
     *
     * @return the bookie authentication provider factory class name or null.
     */
    public String getBookieAuthProviderFactoryClass() {
        return BOOKIE_AUTH_PROVIDER_FACTORY_CLASS_KEY.getString(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ServerConfiguration setNettyMaxFrameSizeBytes(int maxSize) {
        super.setNettyMaxFrameSizeBytes(maxSize);
        return this;
    }

    /**
     * Get the truststore type for client. Default is JKS.
     *
     * @return
     */
    public String getTLSTrustStoreType() {
        return TLS_TRUSTSTORE_TYPE_KEY.getString(this);
    }

    /**
     * Set the keystore type for client.
     *
     * @return
     */
    public ServerConfiguration setTLSKeyStoreType(String arg) {
        TLS_TRUSTSTORE_TYPE_KEY.set(this, arg);
        return this;
    }

    /**
     * Get the keystore path for the client.
     *
     * @return
     */
    public String getTLSKeyStore() {
        return TLS_KEYSTORE_KEY.getString(this);
    }

    /**
     * Set the keystore path for the client.
     *
     * @return
     */
    public ServerConfiguration setTLSKeyStore(String arg) {
        TLS_KEYSTORE_KEY.set(this, arg);
        return this;
    }

    /**
     * Get the path to file containing keystore password if the client keystore is password protected. Default is null.
     *
     * @return
     */
    public String getTLSKeyStorePasswordPath() {
        return TLS_KEYSTORE_PASSWORD_PATH_KEY.getString(this);
    }

    /**
     * Set the path to file containing keystore password, if the client keystore is password protected.
     *
     * @return
     */
    public ServerConfiguration setTLSKeyStorePasswordPath(String arg) {
        TLS_KEYSTORE_PASSWORD_PATH_KEY.set(this, arg);
        return this;
    }

    /**
     * Get the keystore type for client. Default is JKS.
     *
     * @return
     */
    public String getTLSKeyStoreType() {
        return TLS_KEYSTORE_TYPE_KEY.getString(this);
    }

    /**
     * Set the truststore type for client.
     *
     * @return
     */
    public ServerConfiguration setTLSTrustStoreType(String arg) {
        setProperty(TLS_TRUSTSTORE_TYPE, arg);
        return this;
    }

    /**
     * Get the truststore path for the client.
     *
     * @return
     */
    public String getTLSTrustStore() {
        return TLS_TRUSTSTORE_KEY.getString(this);
    }

    /**
     * Set the truststore path for the client.
     *
     * @return
     */
    public ServerConfiguration setTLSTrustStore(String arg) {
        TLS_TRUSTSTORE_KEY.set(this, arg);
        return this;
    }

    /**
     * Get the path to file containing truststore password if the client truststore is password protected. Default is
     * null.
     *
     * @return
     */
    public String getTLSTrustStorePasswordPath() {
        return TLS_TRUSTSTORE_PASSWORD_PATH_KEY.getString(this);
    }

    /**
     * Set the path to file containing truststore password, if the client truststore is password protected.
     *
     * @return
     */
    public ServerConfiguration setTLSTrustStorePasswordPath(String arg) {
        TLS_TRUSTSTORE_PASSWORD_PATH_KEY.set(this, arg);
        return this;
    }

    /**
     * Whether to enable recording task execution stats.
     *
     * @return flag to enable/disable recording task execution stats.
     */
    public boolean getEnableTaskExecutionStats() {
        return ENABLE_TASK_EXECUTION_STATS_KEY.getBoolean(this);
    }

    /**
     * Enable/Disable recording task execution stats.
     *
     * @param enabled
     *          flag to enable/disable recording task execution stats.
     * @return client configuration.
     */
    public ServerConfiguration setEnableTaskExecutionStats(boolean enabled) {
        ENABLE_TASK_EXECUTION_STATS_KEY.set(this, enabled);
        return this;
    }

    /**
     * Gets the minimum safe Usable size to be available in index directory for Bookie to create Index File while
     * replaying journal at the time of Bookie Start in Readonly Mode (in bytes).
     *
     * @return minimum safe usable size to be available in index directory for bookie to create index files.
     * @see #setMinUsableSizeForIndexFileCreation(long)
     */
    public long getMinUsableSizeForIndexFileCreation() {
        return MIN_USABLESIZE_FOR_INDEXFILE_CREATION_KEY.getLong(this);
    }

    /**
     * Sets the minimum safe Usable size to be available in index directory for Bookie to create Index File while
     * replaying journal at the time of Bookie Start in Readonly Mode (in bytes).
     *
     * <p>This parameter allows creating index files when there are enough disk spaces, even when the bookie
     * is running at readonly mode because of the disk usage is exceeding {@link #getDiskUsageThreshold()}. Because
     * compaction, journal replays can still write index files to disks when a bookie is readonly.
     *
     * @param minUsableSizeForIndexFileCreation min usable size for index file creation
     * @return server configuration
     */
    public ServerConfiguration setMinUsableSizeForIndexFileCreation(long minUsableSizeForIndexFileCreation) {
        MIN_USABLESIZE_FOR_INDEXFILE_CREATION_KEY.set(this, minUsableSizeForIndexFileCreation);
        return this;
    }

    /**
     * Gets the minimum safe usable size to be available in ledger directory for Bookie to create entry log files.
     *
     * @return minimum safe usable size to be available in ledger directory for entry log file creation.
     * @see #setMinUsableSizeForEntryLogCreation(long)
     */
    public long getMinUsableSizeForEntryLogCreation() {
        return MIN_USABLESIZE_FOR_ENTRYLOG_CREATION_KEY.getLong(this);
    }

    /**
     * Sets the minimum safe usable size to be available in ledger directory for Bookie to create entry log files.
     *
     * <p>This parameter allows creating entry log files when there are enough disk spaces, even when the bookie
     * is running at readonly mode because of the disk usage is exceeding {@link #getDiskUsageThreshold()}. Because
     * compaction, journal replays can still write data to disks when a bookie is readonly.
     *
     * @param minUsableSizeForEntryLogCreation minimum safe usable size to be available in ledger directory
     * @return server configuration
     */
    public ServerConfiguration setMinUsableSizeForEntryLogCreation(long minUsableSizeForEntryLogCreation) {
        MIN_USABLESIZE_FOR_ENTRYLOG_CREATION_KEY.set(this, minUsableSizeForEntryLogCreation);
        return this;
    }

    /**
     * Gets the minimum safe usable size to be available in ledger directory for Bookie to accept high priority writes.
     *
     * <p>If not set, it is the value of {@link #getMinUsableSizeForEntryLogCreation()}.
     *
     * @return the minimum safe usable size per ledger directory for bookie to accept high priority writes.
     */
    public long getMinUsableSizeForHighPriorityWrites() {
        return MIN_USABLESIZE_FOR_HIGH_PRIORITY_WRITES_KEY.getLong(this);
    }

    /**
     * Sets the minimum safe usable size to be available in ledger directory for Bookie to accept high priority writes.
     *
     * @param minUsableSizeForHighPriorityWrites minimum safe usable size per ledger directory for Bookie to accept
     *                                           high priority writes
     * @return server configuration.
     */
    public ServerConfiguration setMinUsableSizeForHighPriorityWrites(long minUsableSizeForHighPriorityWrites) {
        MIN_USABLESIZE_FOR_HIGH_PRIORITY_WRITES_KEY.set(this, minUsableSizeForHighPriorityWrites);
        return this;
    }

    /**
     * returns whether it is allowed to have multiple ledger/index/journal
     * Directories in the same filesystem diskpartition.
     *
     * @return
     */
    public boolean isAllowMultipleDirsUnderSameDiskPartition() {
        return ALLOW_MULTIPLEDIRS_UNDER_SAME_DISKPARTITION_KEY.getBoolean(this);
    }

    /**
     * Configure the Bookie to allow/disallow multiple ledger/index/journal
     * directories in the same filesystem diskpartition.
     *
     * @param allow
     *
     * @return server configuration object.
     */
    public ServerConfiguration setAllowMultipleDirsUnderSameDiskPartition(boolean allow) {
        ALLOW_MULTIPLEDIRS_UNDER_SAME_DISKPARTITION_KEY.set(this, allow);
        return this;
    }

    /**
     * Get whether to start the http server or not.
     *
     * @return true - if http server should start
     */
    public boolean isHttpServerEnabled() {
        return HTTP_SERVER_ENABLED_KEY.getBoolean(this);
    }

    /**
     * Set whether to start the http server or not.
     *
     * @param enabled
     *            - true if we should start http server
     * @return ServerConfiguration
     */
    public ServerConfiguration setHttpServerEnabled(boolean enabled) {
        HTTP_SERVER_ENABLED_KEY.set(this, enabled);
        return this;
    }

    /**
     * Get the http server port.
     *
     * @return http server port
     */
    public int getHttpServerPort() {
        return HTTP_SERVER_PORT_KEY.getInt(this);
    }

    /**
     * Set Http server port listening on.
     *
     * @param port
     *          Port to listen on
     * @return server configuration
     */
    public ServerConfiguration setHttpServerPort(int port) {
        HTTP_SERVER_PORT_KEY.set(this, port);
        return this;
    }

    /**
     * Get the extra list of server lifecycle components to enable on a bookie server.
     *
     * @return the extra list of server lifecycle components to enable on a bookie server.
     */
    public String[] getExtraServerComponents() {
        return EXTRA_SERVER_COMPONENTS_KEY.getArrayWithoutDefault(this);
    }

    /**
     * Set the extra list of server lifecycle components to enable on a bookie server.
     *
     * @param componentClasses
     *          the list of server lifecycle components to enable on a bookie server.
     * @return server configuration.
     */
    public ServerConfiguration setExtraServerComponents(String[] componentClasses) {
        EXTRA_SERVER_COMPONENTS_KEY.set(this, componentClasses);
        return this;
    }

    /**
     * Return the flag whether to ignore startup failures on loading server components specified at
     * {@link #getExtraServerComponents()}.
     *
     * @return the flag whether to ignore startup failures on loading server components specified at
     * {@link #getExtraServerComponents()}. The default value is <tt>false</tt>.
     */
    public boolean getIgnoreExtraServerComponentsStartupFailures() {
        return IGNORE_EXTRA_SERVER_COMPONENTS_STARTUP_FAILURES_KEY.getBoolean(this);
    }

    /**
     * Set the flag whether to ignore startup failures on loading server components specified at
     * {@link #getExtraServerComponents()}.
     *
     * @param enabled flag to enable/disable ignoring startup failures on loading server components.
     * @return server configuration.
     */
    public ServerConfiguration setIgnoreExtraServerComponentsStartupFailures(boolean enabled) {
        IGNORE_EXTRA_SERVER_COMPONENTS_STARTUP_FAILURES_KEY.set(this, enabled);
        return this;
    }

    /**
     * Get server netty channel write buffer low water mark.
     *
     * @return netty channel write buffer low water mark.
     */
    public int getServerWriteBufferLowWaterMark() {
        return SERVER_WRITEBUFFER_LOW_WATER_MARK_KEY.getInt(this);
    }

    /**
     * Set server netty channel write buffer low water mark.
     *
     * @param waterMark
     *          netty channel write buffer low water mark.
     * @return client configuration.
     */
    public ServerConfiguration setServerWriteBufferLowWaterMark(int waterMark) {
        SERVER_WRITEBUFFER_LOW_WATER_MARK_KEY.set(this, waterMark);
        return this;
    }

    /**
     * Get server netty channel write buffer high water mark.
     *
     * @return netty channel write buffer high water mark.
     */
    public int getServerWriteBufferHighWaterMark() {
        return SERVER_WRITEBUFFER_HIGH_WATER_MARK_KEY.getInt(this);
    }

    /**
     * Set server netty channel write buffer high water mark.
     *
     * @param waterMark
     *          netty channel write buffer high water mark.
     * @return client configuration.
     */
    public ServerConfiguration setServerWriteBufferHighWaterMark(int waterMark) {
        SERVER_WRITEBUFFER_HIGH_WATER_MARK_KEY.set(this, waterMark);
        return this;
    }

    /**
     * Set registration manager class.
     *
     * @param regManagerClass
     *            ManagerClass
     * @deprecated since 4.7.0, in favor of using {@link #setMetadataServiceUri(String)}
     */
    @Deprecated
    public void setRegistrationManagerClass(
            Class<? extends RegistrationManager> regManagerClass) {
        REGISTRATION_MANAGER_CLASS_KEY.set(this, regManagerClass);
    }

    /**
     * Get Registration Manager Class.
     *
     * @return registration manager class.
     * @deprecated since 4.7.0, in favor of using {@link #getMetadataServiceUri()}
     */
    @Deprecated
    public Class<? extends RegistrationManager> getRegistrationManagerClass()
            throws ConfigurationException {
        return REGISTRATION_MANAGER_CLASS_KEY.getClass(this, RegistrationManager.class);
    }

    @Override
    protected ServerConfiguration getThis() {
        return this;
    }

    /*
     * specifies if entryLog per ledger is enabled. If it is enabled, then there
     * would be a active entrylog for each ledger
     */
    public boolean isEntryLogPerLedgerEnabled() {
        return ENTRY_LOG_PER_LEDGER_ENABLED_KEY.getBoolean(this);
    }

    /*
     * enables/disables entrylog per ledger feature.
     *
     */
    public ServerConfiguration setEntryLogPerLedgerEnabled(boolean entryLogPerLedgerEnabled) {
        ENTRY_LOG_PER_LEDGER_ENABLED_KEY.set(this, entryLogPerLedgerEnabled);
        return this;
    }

    /*
     * In the case of multipleentrylogs, multiple threads can be used to flush the memtable.
     *
     * Gets the number of threads used to flush entrymemtable
     */
    public int getNumOfMemtableFlushThreads() {
        return NUMBER_OF_MEMTABLE_FLUSH_THREADS_KEY.getInt(this);
    }

    /*
     * Sets the number of threads used to flush entrymemtable, in the case of multiple entrylogs
     *
     */
    public ServerConfiguration setNumOfMemtableFlushThreads(int numOfMemtableFlushThreads) {
        NUMBER_OF_MEMTABLE_FLUSH_THREADS_KEY.set(this, numOfMemtableFlushThreads);
        return this;
    }

    /*
     * in entryLogPerLedger feature, this specifies the time, once this duration
     * has elapsed after the entry's last access, that entry should be
     * automatically removed from the cache
     */
    public int getEntrylogMapAccessExpiryTimeInSeconds() {
        return ENTRYLOGMAP_ACCESS_EXPIRYTIME_INSECONDS_KEY.getInt(this);
    }

    /*
     * sets the time duration for entrylogMapAccessExpiryTimeInSeconds, which will be used for cache eviction
     * policy, in entrylogperledger feature.
     */
    public ServerConfiguration setEntrylogMapAccessExpiryTimeInSeconds(int entrylogMapAccessExpiryTimeInSeconds) {
        ENTRYLOGMAP_ACCESS_EXPIRYTIME_INSECONDS_KEY.set(this, entrylogMapAccessExpiryTimeInSeconds);
        return this;
    }

    /*
     * get the maximum number of entrylogs that can be active at a given point
     * in time.
     */
    public int getMaximumNumberOfActiveEntryLogs() {
        return MAXIMUM_NUMBER_OF_ACTIVE_ENTRYLOGS_KEY.getInt(this);
    }

    /*
     * sets the maximum number of entrylogs that can be active at a given point
     * in time.
     */
    public ServerConfiguration setMaximumNumberOfActiveEntryLogs(int maximumNumberOfActiveEntryLogs) {
        MAXIMUM_NUMBER_OF_ACTIVE_ENTRYLOGS_KEY.set(this, maximumNumberOfActiveEntryLogs);
        return this;
    }

    /*
     * in EntryLogManagerForEntryLogPerLedger, this config value specifies the
     * metrics cache size limits in multiples of entrylogMap cache size limits.
     */
    public int getEntryLogPerLedgerCounterLimitsMultFactor() {
        return ENTRY_LOG_PER_LEDGER_COUNTER_LIMITS_MULT_FACTOR_KEY.getInt(this);
    }

    /*
     * in EntryLogManagerForEntryLogPerLedger, this config value specifies the
     * metrics cache size limits in multiples of entrylogMap cache size limits.
     */
    public ServerConfiguration setEntryLogPerLedgerCounterLimitsMultFactor(
            int entryLogPerLedgerCounterLimitsMultFactor) {
        ENTRYLOGMAP_ACCESS_EXPIRYTIME_INSECONDS_KEY.set(this, entryLogPerLedgerCounterLimitsMultFactor);
        return this;
    }

    /**
     * True if a local consistency check should be performed on startup.
     */
    public boolean isLocalConsistencyCheckOnStartup() {
        return LOCAL_CONSISTENCY_CHECK_ON_STARTUP_KEY.getBoolean(this);
    }

    /**
     * Limit who can start the application to prevent future permission errors.
     */
    public void setPermittedStartupUsers(String s) {
        PERMITTED_STARTUP_USERS_KEY.set(this, s);
    }

    /**
     * Get array of users specified in this property.
     */
    public String[] getPermittedStartupUsers() {
        return PERMITTED_STARTUP_USERS_KEY.getArray(this);
    }
}
