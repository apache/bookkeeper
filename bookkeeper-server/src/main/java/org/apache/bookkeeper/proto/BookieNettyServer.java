/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.proto;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ExtensionRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;

/**
 * Netty server for serving bookie requests
 */
class BookieNettyServer {
    private final static Logger LOG = LoggerFactory.getLogger(BookieNettyServer.class);

    final static int maxMessageSize = 0xfffff;
    final ServerConfiguration conf;
    final ChannelFactory serverChannelFactory;
    final ChannelFactory jvmServerChannelFactory;
    final RequestProcessor requestProcessor;
    final ChannelGroup allChannels = new CleanupChannelGroup();
    final AtomicBoolean isRunning = new AtomicBoolean(false);
    Object suspensionLock = new Object();
    boolean suspended = false;
    final BookieSocketAddress bookieAddress;

    final BookieAuthProvider.Factory authProviderFactory;
    final BookieProtoEncoding.ResponseEncoder responseEncoder;
    final BookieProtoEncoding.RequestDecoder requestDecoder;

    BookieNettyServer(ServerConfiguration conf, RequestProcessor processor)
            throws IOException, KeeperException, InterruptedException, BookieException  {
        this.conf = conf;
        this.requestProcessor = processor;

        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        authProviderFactory = AuthProviderFactoryFactory.newBookieAuthProviderFactory(conf, registry);

        responseEncoder = new BookieProtoEncoding.ResponseEncoder(registry);
        requestDecoder = new BookieProtoEncoding.RequestDecoder(registry);

        ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
        String base = "bookie-" + conf.getBookiePort() + "-netty";
        serverChannelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(tfb.setNameFormat(base + "-boss-%d").build()),
                Executors.newCachedThreadPool(tfb.setNameFormat(base + "-worker-%d").build()));
        if (conf.isEnableLocalTransport()) {
            jvmServerChannelFactory = new DefaultLocalServerChannelFactory();
        } else {
            jvmServerChannelFactory = null;
        }
        bookieAddress = Bookie.getBookieAddress(conf);
        InetSocketAddress bindAddress;
        if (conf.getListeningInterface() == null) {
            // listen on all interfaces
            bindAddress = new InetSocketAddress(conf.getBookiePort());
        } else {
            bindAddress = bookieAddress.getSocketAddress();
        }
        listenOn(bindAddress, bookieAddress);
    }

    boolean isRunning() {
        return isRunning.get();
    }

    @VisibleForTesting
    void suspendProcessing() {
        synchronized (suspensionLock) {
            suspended = true;
            allChannels.setReadable(false).awaitUninterruptibly();
        }
    }

    @VisibleForTesting
    void resumeProcessing() {
        synchronized (suspensionLock) {
            suspended = false;
            allChannels.setReadable(true).awaitUninterruptibly();
            suspensionLock.notifyAll();
        }
    }

    private void listenOn(InetSocketAddress address, BookieSocketAddress bookieAddress) {
        ServerBootstrap bootstrap = new ServerBootstrap(serverChannelFactory);
        bootstrap.setPipelineFactory(new BookiePipelineFactory());
        bootstrap.setOption("child.tcpNoDelay", conf.getServerTcpNoDelay());
        bootstrap.setOption("child.soLinger", 2);

        Channel listen = bootstrap.bind(address);
        allChannels.add(listen);

        if (conf.isEnableLocalTransport()) {
            ServerBootstrap jvmbootstrap = new ServerBootstrap(jvmServerChannelFactory);
            jvmbootstrap.setPipelineFactory(new BookiePipelineFactory());

            // use the same address 'name', so clients can find local Bookie still discovering them using ZK
            Channel jvmlisten = jvmbootstrap.bind(bookieAddress.getLocalAddress());
            allChannels.add(jvmlisten);
            LocalBookiesRegistry.registerLocalBookieAddress(bookieAddress);
        }
    }

    void start() {
        isRunning.set(true);
    }

    void shutdown() {
        LOG.info("Shutting down BookieNettyServer");
        if (conf.isEnableLocalTransport()) {
            LocalBookiesRegistry.unregisterLocalBookieAddress(bookieAddress);
        }
        isRunning.set(false);
        allChannels.close().awaitUninterruptibly();
        serverChannelFactory.releaseExternalResources();
        if (conf.isEnableLocalTransport()) {
            jvmServerChannelFactory.releaseExternalResources();
        }
    }

    private class BookiePipelineFactory implements ChannelPipelineFactory {
        public ChannelPipeline getPipeline() throws Exception {
            synchronized (suspensionLock) {
                while (suspended) {
                    suspensionLock.wait();
                }
            }
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("lengthbaseddecoder",
                             new LengthFieldBasedFrameDecoder(maxMessageSize, 0, 4, 0, 4));
            pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));

            pipeline.addLast("bookieProtoDecoder", requestDecoder);
            pipeline.addLast("bookieProtoEncoder", responseEncoder);
            pipeline.addLast("bookieAuthHandler",
                             new AuthHandler.ServerSideHandler(authProviderFactory));

            SimpleChannelHandler requestHandler = isRunning.get() ?
                    new BookieRequestHandler(conf, requestProcessor, allChannels)
                    : new RejectRequestHandler();

            pipeline.addLast("bookieRequestHandler", requestHandler);
            return pipeline;
        }
    }

    private static class RejectRequestHandler extends SimpleChannelHandler {

        @Override
        public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            ctx.getChannel().close();
        }

    }

    private static class CleanupChannelGroup extends DefaultChannelGroup {
        private AtomicBoolean closed = new AtomicBoolean(false);

        CleanupChannelGroup() {
            super("BookieChannelGroup");
        }

        @Override
        public boolean add(Channel channel) {
            boolean ret = super.add(channel);
            if (closed.get()) {
                channel.close();
            }
            return ret;
        }

        @Override
        public ChannelGroupFuture close() {
            closed.set(true);
            return super.close();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CleanupChannelGroup)) {
                return false;
            }
            CleanupChannelGroup other = (CleanupChannelGroup)o;
            return other.closed.get() == closed.get()
                && super.equals(other);
        }

        @Override
        public int hashCode() {
            return super.hashCode() * 17 + (closed.get() ? 1 : 0);
        }
    }
}
