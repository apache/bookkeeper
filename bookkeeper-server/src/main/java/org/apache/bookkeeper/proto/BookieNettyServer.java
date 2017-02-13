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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.commons.lang.SystemUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.ExtensionRegistry;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import com.google.common.annotations.VisibleForTesting;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import org.apache.bookkeeper.auth.BookKeeperPrincipal;
import org.apache.bookkeeper.bookie.BookieConnectionPeer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * Netty server for serving bookie requests
 */
class BookieNettyServer {

    private final static Logger LOG = LoggerFactory.getLogger(BookieNettyServer.class);

    final int maxFrameSize;
    final ServerConfiguration conf;
    final EventLoopGroup eventLoopGroup;
    final EventLoopGroup jvmEventLoopGroup;
    final RequestProcessor requestProcessor;
    final AtomicBoolean isRunning = new AtomicBoolean(false);
    final Object suspensionLock = new Object();
    boolean suspended = false;
    ChannelGroup allChannels;
    final BookieSocketAddress bookieAddress;

    final BookieAuthProvider.Factory authProviderFactory;
    final BookieProtoEncoding.ResponseEncoder responseEncoder;
    final BookieProtoEncoding.RequestDecoder requestDecoder;

    BookieNettyServer(ServerConfiguration conf, RequestProcessor processor)
        throws IOException, KeeperException, InterruptedException, BookieException {
        this.maxFrameSize = conf.getNettyMaxFrameSizeBytes();
        this.conf = conf;
        this.requestProcessor = processor;

        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        authProviderFactory = AuthProviderFactoryFactory.newBookieAuthProviderFactory(conf);

        responseEncoder = new BookieProtoEncoding.ResponseEncoder(registry);
        requestDecoder = new BookieProtoEncoding.RequestDecoder(registry);

        if (!conf.isDisableServerSocketBind()) {
            ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("bookie-io-%s").build();
            final int numThreads = Runtime.getRuntime().availableProcessors() * 2;

            EventLoopGroup eventLoopGroup;
            if (SystemUtils.IS_OS_LINUX) {
                try {
                    eventLoopGroup = new EpollEventLoopGroup(numThreads, threadFactory);
                } catch (Throwable t) {
                    LOG.warn("Could not use Netty Epoll event loop for bookie server: {}", t.getMessage());
                    eventLoopGroup = new NioEventLoopGroup(numThreads, threadFactory);
                }
            } else {
                eventLoopGroup = new NioEventLoopGroup(numThreads, threadFactory);
            }

            this.eventLoopGroup = eventLoopGroup;
            allChannels = new CleanupChannelGroup(eventLoopGroup);
        } else {
            this.eventLoopGroup = null;
        }

        if (conf.isEnableLocalTransport()) {
            jvmEventLoopGroup = new DefaultEventLoopGroup();
            allChannels = new CleanupChannelGroup(jvmEventLoopGroup);
        } else {
            jvmEventLoopGroup = null;
        }

        bookieAddress = Bookie.getBookieAddress(conf);
        InetSocketAddress bindAddress;
        if (conf.getListeningInterface() == null) {
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
            for (Channel channel : allChannels) {
                channel.config().setAutoRead(false);
            }
        }
    }

    @VisibleForTesting
    void resumeProcessing() {
        synchronized (suspensionLock) {
            suspended = false;
            for (Channel channel : allChannels) {
                channel.config().setAutoRead(true);
            }
            suspensionLock.notifyAll();
        }
    }

    private void listenOn(InetSocketAddress address, BookieSocketAddress bookieAddress) throws InterruptedException {
        if (!conf.isDisableServerSocketBind()) {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.childOption(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true));
            bootstrap.group(eventLoopGroup, eventLoopGroup);
            bootstrap.childOption(ChannelOption.TCP_NODELAY, conf.getServerTcpNoDelay());
            bootstrap.childOption(ChannelOption.SO_LINGER, conf.getServerSockLinger());
            bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                    new AdaptiveRecvByteBufAllocator(conf.getRecvByteBufAllocatorSizeMin(),
                            conf.getRecvByteBufAllocatorSizeInitial(), conf.getRecvByteBufAllocatorSizeMax()));
    
            if (eventLoopGroup instanceof EpollEventLoopGroup) {
                bootstrap.channel(EpollServerSocketChannel.class);
            } else {
                bootstrap.channel(NioServerSocketChannel.class);
            }
    
            bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    synchronized (suspensionLock) {
                        while (suspended) {
                            suspensionLock.wait();
                        }
                    }

                    BookieSideConnectionPeerContextHandler contextHandler = new BookieSideConnectionPeerContextHandler();
                    ChannelPipeline pipeline = ch.pipeline();
    
                    pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
                    pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
    
                    pipeline.addLast("bookieProtoDecoder", requestDecoder);
                    pipeline.addLast("bookieProtoEncoder", responseEncoder);
                    pipeline.addLast("bookieAuthHandler", new AuthHandler.ServerSideHandler(contextHandler.getConnectionPeer(), authProviderFactory));
    
                    ChannelInboundHandler requestHandler = isRunning.get()
                            ? new BookieRequestHandler(conf, requestProcessor, allChannels) : new RejectRequestHandler();
                    pipeline.addLast("bookieRequestHandler", requestHandler);
    
                }
            });
    
            // Bind and start to accept incoming connections
            bootstrap.bind(address.getAddress(), address.getPort()).sync();
        }

        if (conf.isEnableLocalTransport()) {
            ServerBootstrap jvmBootstrap = new ServerBootstrap();
            jvmBootstrap.childOption(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true));
            jvmBootstrap.group(jvmEventLoopGroup, jvmEventLoopGroup);
            jvmBootstrap.childOption(ChannelOption.TCP_NODELAY, conf.getServerTcpNoDelay());
            jvmBootstrap.childOption(ChannelOption.SO_KEEPALIVE, conf.getServerSockKeepalive());
            jvmBootstrap.childOption(ChannelOption.SO_LINGER, conf.getServerSockLinger());
            jvmBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                    new AdaptiveRecvByteBufAllocator(conf.getRecvByteBufAllocatorSizeMin(),
                            conf.getRecvByteBufAllocatorSizeInitial(), conf.getRecvByteBufAllocatorSizeMax()));

            if (jvmEventLoopGroup instanceof DefaultEventLoopGroup) {
                jvmBootstrap.channel(LocalServerChannel.class);
            } else if (jvmEventLoopGroup instanceof EpollEventLoopGroup) {
                jvmBootstrap.channel(EpollServerSocketChannel.class);
            } else {
                jvmBootstrap.channel(NioServerSocketChannel.class);
            }

            jvmBootstrap.childHandler(new ChannelInitializer<LocalChannel>() {
                @Override
                protected void initChannel(LocalChannel ch) throws Exception {
                    synchronized (suspensionLock) {
                        while (suspended) {
                            suspensionLock.wait();
                        }
                    }

                    BookieSideConnectionPeerContextHandler contextHandler = new BookieSideConnectionPeerContextHandler();
                    ChannelPipeline pipeline = ch.pipeline();

                    pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
                    pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));

                    pipeline.addLast("bookieProtoDecoder", requestDecoder);
                    pipeline.addLast("bookieProtoEncoder", responseEncoder);
                    pipeline.addLast("bookieAuthHandler", new AuthHandler.ServerSideHandler(contextHandler.getConnectionPeer(), authProviderFactory));

                    ChannelInboundHandler requestHandler = isRunning.get()
                            ? new BookieRequestHandler(conf, requestProcessor, allChannels) : new RejectRequestHandler();
                    pipeline.addLast("bookieRequestHandler", requestHandler);

                }
            });

            // use the same address 'name', so clients can find local Bookie still discovering them using ZK
            jvmBootstrap.bind(bookieAddress.getLocalAddress()).sync();
            LocalBookiesRegistry.registerLocalBookieAddress(bookieAddress);
        }
    }

    void start() {
        isRunning.set(true);
    }

    void shutdown() {
        LOG.info("Shutting down BookieNettyServer");
        isRunning.set(false);
        allChannels.close().awaitUninterruptibly();

        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
        }
        if (jvmEventLoopGroup != null) {
            jvmEventLoopGroup.shutdownGracefully();
            LocalBookiesRegistry.registerLocalBookieAddress(bookieAddress);
	}

        authProviderFactory.close();
    }

    class BookieSideConnectionPeerContextHandler extends ChannelInboundHandlerAdapter {

        final BookieConnectionPeer connectionPeer;
        volatile Channel channel;
        volatile BookKeeperPrincipal authorizedId = BookKeeperPrincipal.ANONYMOUS;

        public BookieSideConnectionPeerContextHandler() {
            this.connectionPeer = new BookieConnectionPeer() {
                @Override
                public SocketAddress getRemoteAddr() {
                    Channel c = channel;
                    if (c != null) {
                        return c.remoteAddress();
                    } else {
                        return null;
                    }
                }

                @Override
                public Collection<Object> getProtocolPrincipals() {
                    return Collections.emptyList();
                }

                @Override
                public void disconnect() {
                    Channel c = channel;
                    if (c != null) {
                        c.close();
                    }
                    LOG.info("authplugin disconnected channel {}", channel);
                }

                @Override
                public BookKeeperPrincipal getAuthorizedId() {
                    return authorizedId;
                }

                @Override
                public void setAuthorizedId(BookKeeperPrincipal principal) {
                    LOG.info("connection {} authenticated as {}", channel, principal);
                    authorizedId = principal;
                }

            };
        }

        public BookieConnectionPeer getConnectionPeer() {
            return connectionPeer;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            channel = ctx.channel();
        }

    }

    private static class RejectRequestHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.channel().close();
        }
    }

    private static class CleanupChannelGroup extends DefaultChannelGroup {

        private AtomicBoolean closed = new AtomicBoolean(false);

        public CleanupChannelGroup(EventLoopGroup eventLoopGroup) {
            super("BookieChannelGroup", eventLoopGroup.next());
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
            CleanupChannelGroup other = (CleanupChannelGroup) o;
            return other.closed.get() == closed.get()
                && super.equals(other);
        }

        @Override
        public int hashCode() {
            return super.hashCode() * 17 + (closed.get() ? 1 : 0);
        }
    }
}
