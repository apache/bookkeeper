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
package org.apache.hedwig.server.netty;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BKException;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.common.TerminateJVMExceptionHandler;
import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.delivery.FIFODeliveryManager;
import org.apache.hedwig.server.handlers.CloseSubscriptionHandler;
import org.apache.hedwig.server.handlers.ConsumeHandler;
import org.apache.hedwig.server.handlers.Handler;
import org.apache.hedwig.server.handlers.NettyHandlerBean;
import org.apache.hedwig.server.handlers.PublishHandler;
import org.apache.hedwig.server.handlers.SubscribeHandler;
import org.apache.hedwig.server.handlers.SubscriptionChannelManager;
import org.apache.hedwig.server.handlers.SubscriptionChannelManager.SubChannelDisconnectedListener;
import org.apache.hedwig.server.handlers.UnsubscribeHandler;
import org.apache.hedwig.server.jmx.HedwigMBeanRegistry;
import org.apache.hedwig.server.meta.MetadataManagerFactory;
import org.apache.hedwig.server.meta.ZkMetadataManagerFactory;
import org.apache.hedwig.server.persistence.BookkeeperPersistenceManager;
import org.apache.hedwig.server.persistence.LocalDBPersistenceManager;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.persistence.PersistenceManagerWithRangeScan;
import org.apache.hedwig.server.persistence.ReadAheadCache;
import org.apache.hedwig.server.regions.HedwigHubClientFactory;
import org.apache.hedwig.server.regions.RegionManager;
import org.apache.hedwig.server.ssl.SslServerContextFactory;
import org.apache.hedwig.server.subscriptions.InMemorySubscriptionManager;
import org.apache.hedwig.server.subscriptions.SubscriptionManager;
import org.apache.hedwig.server.subscriptions.MMSubscriptionManager;
import org.apache.hedwig.server.topics.MMTopicManager;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.TrivialOwnAllTopicManager;
import org.apache.hedwig.server.topics.ZkTopicManager;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.apache.hedwig.util.Either;
import org.apache.hedwig.zookeeper.SafeAsyncCallback;

public class PubSubServer {

    static Logger logger = LoggerFactory.getLogger(PubSubServer.class);

    private static final String JMXNAME_PREFIX = "PubSubServer_";

    // Netty related variables
    ServerSocketChannelFactory serverChannelFactory;
    ClientSocketChannelFactory clientChannelFactory;
    ServerConfiguration conf;
    org.apache.hedwig.client.conf.ClientConfiguration clientConfiguration;
    ChannelGroup allChannels;

    // Manager components that make up the PubSubServer
    PersistenceManager pm;
    DeliveryManager dm;
    TopicManager tm;
    SubscriptionManager sm;
    RegionManager rm;

    // Metadata Manager Factory
    MetadataManagerFactory mm;

    ZooKeeper zk; // null if we are in standalone mode
    BookKeeper bk; // null if we are in standalone mode

    // we use this to prevent long stack chains from building up in callbacks
    ScheduledExecutorService scheduler;

    // JMX Beans
    NettyHandlerBean jmxNettyBean;
    PubSubServerBean jmxServerBean;
    final ThreadGroup tg;

    protected PersistenceManager instantiatePersistenceManager(TopicManager topicMgr) throws IOException,
        InterruptedException {

        PersistenceManagerWithRangeScan underlyingPM;

        if (conf.isStandalone()) {

            underlyingPM = LocalDBPersistenceManager.instance();

        } else {
            try {
                ClientConfiguration bkConf = new ClientConfiguration();
                bkConf.addConfiguration(conf.getConf());
                bk = new BookKeeper(bkConf, zk, clientChannelFactory);
            } catch (KeeperException e) {
                logger.error("Could not instantiate bookkeeper client", e);
                throw new IOException(e);
            }
            underlyingPM = new BookkeeperPersistenceManager(bk, mm, topicMgr, conf, scheduler);

        }

        PersistenceManager pm = underlyingPM;

        if (conf.getReadAheadEnabled()) {
            pm = new ReadAheadCache(underlyingPM, conf).start();
        }

        return pm;
    }

    protected SubscriptionManager instantiateSubscriptionManager(TopicManager tm, PersistenceManager pm,
                                                                 DeliveryManager dm) {
        if (conf.isStandalone()) {
            return new InMemorySubscriptionManager(conf, tm, pm, dm, scheduler);
        } else {
            return new MMSubscriptionManager(conf, mm, tm, pm, dm, scheduler);
        }

    }

    protected RegionManager instantiateRegionManager(PersistenceManager pm, ScheduledExecutorService scheduler) {
        return new RegionManager(pm, conf, zk, scheduler, new HedwigHubClientFactory(conf, clientConfiguration,
                clientChannelFactory));
    }

    protected void instantiateZookeeperClient() throws Exception {
        if (!conf.isStandalone()) {
            final CountDownLatch signalZkReady = new CountDownLatch(1);

            zk = new ZooKeeper(conf.getZkHost(), conf.getZkTimeout(), new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if(Event.KeeperState.SyncConnected.equals(event.getState())) {
                        signalZkReady.countDown();
                    }
                }
            });
            // wait until connection is effective
            if (!signalZkReady.await(conf.getZkTimeout()*2, TimeUnit.MILLISECONDS)) {
                logger.error("Could not establish connection with ZooKeeper after zk_timeout*2 = " +
                             conf.getZkTimeout()*2 + " ms. (Default value for zk_timeout is 2000).");
                throw new Exception("Could not establish connection with ZooKeeper after zk_timeout*2 = " +
                                    conf.getZkTimeout()*2 + " ms. (Default value for zk_timeout is 2000).");
            }
        }
    }

    protected void instantiateMetadataManagerFactory() throws Exception {
        if (conf.isStandalone()) {
            return;
        }
        mm = MetadataManagerFactory.newMetadataManagerFactory(conf, zk);
    }

    protected TopicManager instantiateTopicManager() throws IOException {
        TopicManager tm;

        if (conf.isStandalone()) {
            tm = new TrivialOwnAllTopicManager(conf, scheduler);
        } else {
            try {
                if (conf.isMetadataManagerBasedTopicManagerEnabled()) {
                    tm = new MMTopicManager(conf, zk, mm, scheduler);
                } else {
                    if (!(mm instanceof ZkMetadataManagerFactory)) {
                        throw new IOException("Uses " + mm.getClass().getName() + " to store hedwig metadata, "
                                            + "but uses zookeeper ephemeral znodes to store topic ownership. "
                                            + "Check your configuration as this could lead to scalability issues.");
                    }
                    tm = new ZkTopicManager(zk, conf, scheduler);
                }
            } catch (PubSubException e) {
                logger.error("Could not instantiate TopicOwnershipManager based topic manager", e);
                throw new IOException(e);
            }
        }
        return tm;
    }

   protected Map<OperationType, Handler> initializeNettyHandlers(
           TopicManager tm, DeliveryManager dm,
           PersistenceManager pm, SubscriptionManager sm,
           SubscriptionChannelManager subChannelMgr) {
        Map<OperationType, Handler> handlers = new HashMap<OperationType, Handler>();
        handlers.put(OperationType.PUBLISH, new PublishHandler(tm, pm, conf));
        handlers.put(OperationType.SUBSCRIBE,
                     new SubscribeHandler(conf, tm, dm, pm, sm, subChannelMgr));
        handlers.put(OperationType.UNSUBSCRIBE,
                     new UnsubscribeHandler(conf, tm, sm, dm, subChannelMgr));
        handlers.put(OperationType.CONSUME, new ConsumeHandler(tm, sm, conf));
        handlers.put(OperationType.CLOSESUBSCRIPTION,
                     new CloseSubscriptionHandler(conf, tm, sm, dm, subChannelMgr));
        handlers = Collections.unmodifiableMap(handlers);
        return handlers;
    }

    protected void initializeNetty(SslServerContextFactory sslFactory,
                                   Map<OperationType, Handler> handlers,
                                   SubscriptionChannelManager subChannelMgr) {
        boolean isSSLEnabled = (sslFactory != null) ? true : false;
        InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
        ServerBootstrap bootstrap = new ServerBootstrap(serverChannelFactory);
        UmbrellaHandler umbrellaHandler =
            new UmbrellaHandler(allChannels, handlers, subChannelMgr, isSSLEnabled);
        PubSubServerPipelineFactory pipeline =
            new PubSubServerPipelineFactory(umbrellaHandler, sslFactory,
                                            conf.getMaximumMessageSize());

        bootstrap.setPipelineFactory(pipeline);
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("reuseAddress", true);

        // Bind and start to accept incoming connections.
        allChannels.add(bootstrap.bind(isSSLEnabled ? new InetSocketAddress(conf.getSSLServerPort())
                                       : new InetSocketAddress(conf.getServerPort())));
        logger.info("Going into receive loop");
    }

    public void shutdown() {
        // TODO: tell bk to close logs

        // Stop topic manager first since it is core of Hub server
        tm.stop();

        // Stop the RegionManager.
        rm.stop();

        // Stop the DeliveryManager and ReadAheadCache threads (if
        // applicable).
        dm.stop();
        pm.stop();

        // Stop the SubscriptionManager if needed.
        sm.stop();

        // Shutdown metadata manager if needed
        if (null != mm) {
            try {
                mm.shutdown();
            } catch (IOException ie) {
                logger.error("Error while shutdown metadata manager factory!", ie);
            }
        }

        // Shutdown the ZooKeeper and BookKeeper clients only if we are
        // not in stand-alone mode.
        try {
            if (bk != null)
                bk.close();
            if (zk != null)
                zk.close();
        } catch (InterruptedException e) {
            logger.error("Error while closing ZooKeeper client : ", e);
        } catch (BKException bke) {
            logger.error("Error while closing BookKeeper client : ", bke);
        }

        // Close and release the Netty channels and resources
        allChannels.close().awaitUninterruptibly();
        serverChannelFactory.releaseExternalResources();
        clientChannelFactory.releaseExternalResources();
        scheduler.shutdown();

        // unregister jmx
        unregisterJMX();
    }

    protected void registerJMX(SubscriptionChannelManager subChannelMgr) {
        try {
            String jmxName = JMXNAME_PREFIX + conf.getServerPort() + "_"
                                            + conf.getSSLServerPort();
            jmxServerBean = new PubSubServerBean(jmxName);
            HedwigMBeanRegistry.getInstance().register(jmxServerBean, null);
            try {
                jmxNettyBean = new NettyHandlerBean(subChannelMgr);
                HedwigMBeanRegistry.getInstance().register(jmxNettyBean, jmxServerBean);
            } catch (Exception e) {
                logger.warn("Failed to register with JMX", e);
                jmxNettyBean = null;
            }
        } catch (Exception e) {
            logger.warn("Failed to register with JMX", e);
            jmxServerBean = null;
        }
        if (pm instanceof ReadAheadCache) {
            ((ReadAheadCache)pm).registerJMX(jmxServerBean);
        }
    }

    protected void unregisterJMX() {
        if (pm != null && pm instanceof ReadAheadCache) {
            ((ReadAheadCache)pm).unregisterJMX();
        }
        try {
            if (jmxNettyBean != null) {
                HedwigMBeanRegistry.getInstance().unregister(jmxNettyBean);
            }
        } catch (Exception e) {
            logger.warn("Failed to unregister with JMX", e);
        }
        try {
            if (jmxServerBean != null) {
                HedwigMBeanRegistry.getInstance().unregister(jmxServerBean);
            }
        } catch (Exception e) {
            logger.warn("Failed to unregister with JMX", e);
        }
        jmxNettyBean = null;
        jmxServerBean = null;
    }

    /**
     * Starts the hedwig server on the given port
     *
     * @param port
     * @throws ConfigurationException
     *             if there is something wrong with the given configuration
     * @throws IOException
     * @throws InterruptedException
     * @throws ConfigurationException
     */
    public PubSubServer(final ServerConfiguration serverConfiguration,
                        final org.apache.hedwig.client.conf.ClientConfiguration clientConfiguration,
                        final Thread.UncaughtExceptionHandler exceptionHandler)
            throws ConfigurationException {

        // First validate the serverConfiguration
        this.conf = serverConfiguration;
        serverConfiguration.validate();

        // Validate the client configuration
        this.clientConfiguration = clientConfiguration;
        clientConfiguration.validate();

        // We need a custom thread group, so that we can override the uncaught
        // exception method
        tg = new ThreadGroup("hedwig") {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                exceptionHandler.uncaughtException(t, e);
            }
        };
        // ZooKeeper threads register their own handler. But if some work that
        // we do in ZK threads throws an exception, we want our handler to be
        // called, not theirs.
        SafeAsyncCallback.setUncaughtExceptionHandler(exceptionHandler);
    }

    public void start() throws Exception {
        final SynchronousQueue<Either<Object, Exception>> queue = new SynchronousQueue<Either<Object, Exception>>();

        new Thread(tg, new Runnable() {
            @Override
            public void run() {
                try {
                    // Since zk is needed by almost everyone,try to see if we
                    // need that first
                    scheduler = Executors.newSingleThreadScheduledExecutor();
                    serverChannelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors
                            .newCachedThreadPool());
                    clientChannelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors
                            .newCachedThreadPool());

                    instantiateZookeeperClient();
                    instantiateMetadataManagerFactory();
                    tm = instantiateTopicManager();
                    pm = instantiatePersistenceManager(tm);
                    dm = new FIFODeliveryManager(tm, pm, conf);
                    dm.start();

                    sm = instantiateSubscriptionManager(tm, pm, dm);
                    rm = instantiateRegionManager(pm, scheduler);
                    sm.addListener(rm);

                    allChannels = new DefaultChannelGroup("hedwig");
                    // Initialize the Netty Handlers (used by the
                    // UmbrellaHandler) once so they can be shared by
                    // both the SSL and non-SSL channels.
                    SubscriptionChannelManager subChannelMgr = new SubscriptionChannelManager();
                    subChannelMgr.addSubChannelDisconnectedListener((SubChannelDisconnectedListener) dm);
                    Map<OperationType, Handler> handlers =
                        initializeNettyHandlers(tm, dm, pm, sm, subChannelMgr);
                    // Initialize Netty for the regular non-SSL channels
                    initializeNetty(null, handlers, subChannelMgr);
                    if (conf.isSSLEnabled()) {
                        initializeNetty(new SslServerContextFactory(conf),
                                        handlers, subChannelMgr);
                    }
                    // register jmx
                    registerJMX(subChannelMgr);
                } catch (Exception e) {
                    ConcurrencyUtils.put(queue, Either.right(e));
                    return;
                }

                ConcurrencyUtils.put(queue, Either.of(new Object(), (Exception) null));
            }

        }).start();

        Either<Object, Exception> either = ConcurrencyUtils.take(queue);
        if (either.left() == null) {
            throw either.right();
        }
    }

    public PubSubServer(ServerConfiguration serverConfiguration,
                        org.apache.hedwig.client.conf.ClientConfiguration clientConfiguration) throws Exception {
        this(serverConfiguration, clientConfiguration, new TerminateJVMExceptionHandler());
    }

    public PubSubServer(ServerConfiguration serverConfiguration) throws Exception {
        this(serverConfiguration, new org.apache.hedwig.client.conf.ClientConfiguration());
    }

    @VisibleForTesting
    public DeliveryManager getDeliveryManager() {
        return dm;
    }

    /**
     *
     * @param msg
     * @param rc
     *            : code to exit with
     */
    public static void errorMsgAndExit(String msg, Throwable t, int rc) {
        logger.error(msg, t);
        System.err.println(msg);
        System.exit(rc);
    }

    public final static int RC_INVALID_CONF_FILE = 1;
    public final static int RC_MISCONFIGURED = 2;
    public final static int RC_OTHER = 3;

    /**
     * @param args
     */
    public static void main(String[] args) {

        logger.info("Attempting to start Hedwig");
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        // The client configuration for the hedwig client in the region manager.
        org.apache.hedwig.client.conf.ClientConfiguration regionMgrClientConfiguration
                = new org.apache.hedwig.client.conf.ClientConfiguration();
        if (args.length > 0) {
            String confFile = args[0];
            try {
                serverConfiguration.loadConf(new File(confFile).toURI().toURL());
            } catch (MalformedURLException e) {
                String msg = "Could not open server configuration file: " + confFile;
                errorMsgAndExit(msg, e, RC_INVALID_CONF_FILE);
            } catch (ConfigurationException e) {
                String msg = "Malformed server configuration file: " + confFile;
                errorMsgAndExit(msg, e, RC_MISCONFIGURED);
            }
            logger.info("Using configuration file " + confFile);
        }
        if (args.length > 1) {
            // args[1] is the client configuration file.
            String confFile = args[1];
            try {
                regionMgrClientConfiguration.loadConf(new File(confFile).toURI().toURL());
            } catch (MalformedURLException e) {
                String msg = "Could not open client configuration file: " + confFile;
                errorMsgAndExit(msg, e, RC_INVALID_CONF_FILE);
            } catch (ConfigurationException e) {
                String msg = "Malformed client configuration file: " + confFile;
                errorMsgAndExit(msg, e, RC_MISCONFIGURED);
            }
        }
        try {
            new PubSubServer(serverConfiguration, regionMgrClientConfiguration).start();
        } catch (Throwable t) {
            errorMsgAndExit("Error during startup", t, RC_OTHER);
        }
    }
}
