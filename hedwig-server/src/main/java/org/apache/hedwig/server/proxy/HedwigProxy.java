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
package org.apache.hedwig.server.proxy;

import java.io.File;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;

import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.server.common.TerminateJVMExceptionHandler;
import org.apache.hedwig.server.handlers.ChannelDisconnectListener;
import org.apache.hedwig.server.handlers.Handler;
import org.apache.hedwig.server.netty.PubSubServer;
import org.apache.hedwig.server.netty.PubSubServerPipelineFactory;
import org.apache.hedwig.server.netty.UmbrellaHandler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class HedwigProxy {
    private static final Logger logger = LoggerFactory.getLogger(HedwigProxy.class);

    HedwigClient client;
    ServerSocketChannelFactory serverSocketChannelFactory;
    ChannelGroup allChannels;
    Map<OperationType, Handler> handlers;
    ProxyConfiguration cfg;
    ChannelTracker tracker;
    ThreadGroup tg;

    public HedwigProxy(final ProxyConfiguration cfg, final UncaughtExceptionHandler exceptionHandler) {
        this.cfg = cfg;

        tg = new ThreadGroup("hedwigproxy") {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                exceptionHandler.uncaughtException(t, e);
            }
        };
    }

    public HedwigProxy(ProxyConfiguration conf) throws InterruptedException {
        this(conf, new TerminateJVMExceptionHandler());
    }

    public void start() throws InterruptedException {
        final LinkedBlockingQueue<Boolean> queue = new LinkedBlockingQueue<Boolean>();

        new Thread(tg, new Runnable() {
            @Override
            public void run() {
                client = new HedwigClient(cfg);
                ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
                serverSocketChannelFactory = new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(tfb.setNameFormat(
                                "HedwigProxy-NIOBoss-%d").build()),
                        Executors.newCachedThreadPool(tfb.setNameFormat(
                                "HedwigProxy-NIOWorker-%d").build()));
                initializeHandlers();
                initializeNetty();

                queue.offer(true);
            }
        }).start();

        queue.take();
    }

    // used for testing
    public ChannelTracker getChannelTracker() {
        return tracker;
    }

    protected void initializeHandlers() {
        handlers = new HashMap<OperationType, Handler>();
        tracker = new ChannelTracker(client.getSubscriber());

        handlers.put(OperationType.PUBLISH, new ProxyPublishHander(client.getPublisher()));
        handlers.put(OperationType.SUBSCRIBE, new ProxySubscribeHandler(client.getSubscriber(), tracker));
        handlers.put(OperationType.UNSUBSCRIBE, new ProxyUnsubscribeHandler(client.getSubscriber(), tracker));
        handlers.put(OperationType.CONSUME, new ProxyConsumeHandler(client.getSubscriber()));
        handlers.put(OperationType.STOP_DELIVERY, new ProxyStopDeliveryHandler(client.getSubscriber(), tracker));
        handlers.put(OperationType.START_DELIVERY, new ProxyStartDeliveryHandler(client.getSubscriber(), tracker));
        handlers.put(OperationType.CLOSESUBSCRIPTION,
                     new ProxyCloseSubscriptionHandler(client.getSubscriber(), tracker));
    }

    protected void initializeNetty() {
        InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
        allChannels = new DefaultChannelGroup("hedwigproxy");
        ServerBootstrap bootstrap = new ServerBootstrap(serverSocketChannelFactory);
        ChannelDisconnectListener disconnectListener =
            (ChannelDisconnectListener) handlers.get(OperationType.SUBSCRIBE);
        UmbrellaHandler umbrellaHandler =
            new UmbrellaHandler(allChannels, handlers, disconnectListener, false);
        PubSubServerPipelineFactory pipeline = new PubSubServerPipelineFactory(umbrellaHandler, null, cfg
                .getMaximumMessageSize());

        bootstrap.setPipelineFactory(pipeline);
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("reuseAddress", true);

        // Bind and start to accept incoming connections.
        allChannels.add(bootstrap.bind(new InetSocketAddress(cfg.getProxyPort())));
        logger.info("Going into receive loop");
    }

    public void shutdown() {
        allChannels.close().awaitUninterruptibly();
        client.close();
        serverSocketChannelFactory.releaseExternalResources();
    }

    // the following method only exists for unit-testing purposes, should go
    // away once we make start delivery totally server-side
    public Handler getStartDeliveryHandler() {
        return handlers.get(OperationType.START_DELIVERY);
    }

    public Handler getStopDeliveryHandler() {
        return handlers.get(OperationType.STOP_DELIVERY);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {

        logger.info("Attempting to start Hedwig Proxy");
        ProxyConfiguration conf = new ProxyConfiguration();
        if (args.length > 0) {
            String confFile = args[0];
            try {
                conf.loadConf(new File(confFile).toURI().toURL());
            } catch (MalformedURLException e) {
                String msg = "Could not open configuration file: " + confFile;
                PubSubServer.errorMsgAndExit(msg, e, PubSubServer.RC_INVALID_CONF_FILE);
            } catch (ConfigurationException e) {
                String msg = "Malformed configuration file: " + confFile;
                PubSubServer.errorMsgAndExit(msg, e, PubSubServer.RC_MISCONFIGURED);
            }
            logger.info("Using configuration file " + confFile);
        }
        try {
            new HedwigProxy(conf).start();
        } catch (Throwable t) {
            PubSubServer.errorMsgAndExit("Error during startup", t, PubSubServer.RC_OTHER);
        }
    }

}
