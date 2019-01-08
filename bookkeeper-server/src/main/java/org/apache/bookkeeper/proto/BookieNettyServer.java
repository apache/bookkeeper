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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ExtensionRegistry;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLPeerUnverifiedException;

import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.auth.BookKeeperPrincipal;
import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.common.collections.BlockingMpscQueue;
import org.apache.bookkeeper.common.util.affinity.CpuAffinity;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.EventLoopUtil;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty server for serving bookie requests.
 */
class BookieNettyServer {

    private static final Logger LOG = LoggerFactory.getLogger(BookieNettyServer.class);

    final int maxFrameSize;
    final ServerConfiguration conf;
    final EventLoopGroup eventLoopGroup;
    final EventLoopGroup jvmEventLoopGroup;
    RequestProcessor requestProcessor;
    final AtomicBoolean isRunning = new AtomicBoolean(false);
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Object suspensionLock = new Object();
    volatile boolean suspended = false;
    ChannelGroup allChannels;
    final BookieSocketAddress bookieAddress;
    final InetSocketAddress bindAddress;

    final BookieAuthProvider.Factory authProviderFactory;
    final ExtensionRegistry registry = ExtensionRegistry.newInstance();

    private final ByteBufAllocator allocator;

    BookieNettyServer(ServerConfiguration conf, RequestProcessor processor, ByteBufAllocator allocator)
        throws IOException, KeeperException, InterruptedException, BookieException {
        this.allocator = allocator;
        this.maxFrameSize = conf.getNettyMaxFrameSizeBytes();
        this.conf = conf;
        this.requestProcessor = processor;
        this.authProviderFactory = AuthProviderFactoryFactory.newBookieAuthProviderFactory(conf);

        if (!conf.isDisableServerSocketBind()) {
            this.eventLoopGroup = EventLoopUtil.getServerEventLoopGroup(conf, new DefaultThreadFactory("bookie-io"));
            allChannels = new CleanupChannelGroup(eventLoopGroup);
        } else {
            this.eventLoopGroup = null;
        }

        if (conf.isEnableLocalTransport()) {
            jvmEventLoopGroup = new DefaultEventLoopGroup(conf.getServerNumIOThreads()) {
                @Override
                protected EventLoop newChild(Executor executor, Object... args) throws Exception {
                    return new DefaultEventLoop(this, executor) {
                        @Override
                        protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
                            if (conf.isBusyWaitEnabled()) {
                                return new BlockingMpscQueue<>(Math.min(maxPendingTasks, 10_000));
                            } else {
                                return super.newTaskQueue(maxPendingTasks);
                            }
                        }
                    };
                }
            };

            // Enable CPU affinity on IO threads
            if (conf.isBusyWaitEnabled()) {
                for (int i = 0; i < conf.getServerNumIOThreads(); i++) {
                    jvmEventLoopGroup.next().submit(() -> {
                        try {
                            CpuAffinity.acquireCore();
                        } catch (Throwable t) {
                            LOG.warn("Failed to acquire CPU core for thread {}", Thread.currentThread().getName(),
                                    t.getMessage(), t);
                        }
                    });
                }
            }

            allChannels = new CleanupChannelGroup(jvmEventLoopGroup);
        } else {
            jvmEventLoopGroup = null;
        }

        bookieAddress = Bookie.getBookieAddress(conf);
        if (conf.getListeningInterface() == null) {
            bindAddress = new InetSocketAddress(conf.getBookiePort());
        } else {
            bindAddress = bookieAddress.getSocketAddress();
        }
        listenOn(bindAddress, bookieAddress);
    }

    public BookieNettyServer setRequestProcessor(RequestProcessor processor) {
        this.requestProcessor = processor;
        return this;
    }

    boolean isRunning() {
        return isRunning.get();
    }

    @VisibleForTesting
    void suspendProcessing() {
        synchronized (suspensionLock) {
            suspended = true;
            for (Channel channel : allChannels) {
                // To suspend processing in the bookie, submit a task
                // that keeps the event loop busy until resume is
                // explicitely invoked
                channel.eventLoop().submit(() -> {
                    while (suspended && isRunning()) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
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
                    Channel c = channel;
                    if (c == null) {
                        return Collections.emptyList();
                    } else {
                        SslHandler ssl = c.pipeline().get(SslHandler.class);
                        if (ssl == null) {
                            return Collections.emptyList();
                        }
                        try {
                            Certificate[] certificates = ssl.engine().getSession().getPeerCertificates();
                            if (certificates == null) {
                                return Collections.emptyList();
                            }
                            List<Object> result = new ArrayList<>();
                            result.addAll(Arrays.asList(certificates));
                            return result;
                        } catch (SSLPeerUnverifiedException err) {
                            LOG.error("Failed to get peer certificates", err);
                            return Collections.emptyList();
                        }

                    }
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

                @Override
                public boolean isSecure() {
                    Channel c = channel;
                    if (c == null) {
                        return false;
                    } else {
                        return c.pipeline().get("tls") != null;
                    }
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

    private void listenOn(InetSocketAddress address, BookieSocketAddress bookieAddress) throws InterruptedException {
        if (!conf.isDisableServerSocketBind()) {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.option(ChannelOption.ALLOCATOR, allocator);
            bootstrap.childOption(ChannelOption.ALLOCATOR, allocator);
            bootstrap.group(eventLoopGroup, eventLoopGroup);
            bootstrap.childOption(ChannelOption.TCP_NODELAY, conf.getServerTcpNoDelay());
            bootstrap.childOption(ChannelOption.SO_LINGER, conf.getServerSockLinger());
            bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                    new AdaptiveRecvByteBufAllocator(conf.getRecvByteBufAllocatorSizeMin(),
                            conf.getRecvByteBufAllocatorSizeInitial(), conf.getRecvByteBufAllocatorSizeMax()));
            bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                    conf.getServerWriteBufferLowWaterMark(), conf.getServerWriteBufferHighWaterMark()));

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

                    BookieSideConnectionPeerContextHandler contextHandler =
                        new BookieSideConnectionPeerContextHandler();
                    ChannelPipeline pipeline = ch.pipeline();

                    // For ByteBufList, skip the usual LengthFieldPrepender and have the encoder itself to add it
                    pipeline.addLast("bytebufList", ByteBufList.ENCODER_WITH_SIZE);

                    pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
                    pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));

                    pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.RequestDecoder(registry));
                    pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.ResponseEncoder(registry));
                    pipeline.addLast("bookieAuthHandler", new AuthHandler.ServerSideHandler(
                                contextHandler.getConnectionPeer(), authProviderFactory));

                    ChannelInboundHandler requestHandler = isRunning.get()
                            ? new BookieRequestHandler(conf, requestProcessor, allChannels)
                            : new RejectRequestHandler();
                    pipeline.addLast("bookieRequestHandler", requestHandler);

                    pipeline.addLast("contextHandler", contextHandler);
                }
            });

            // Bind and start to accept incoming connections
            Channel listen = bootstrap.bind(address.getAddress(), address.getPort()).sync().channel();
            if (listen.localAddress() instanceof InetSocketAddress) {
                if (conf.getBookiePort() == 0) {
                    conf.setBookiePort(((InetSocketAddress) listen.localAddress()).getPort());
                }
            }
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
            jvmBootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                    conf.getServerWriteBufferLowWaterMark(), conf.getServerWriteBufferHighWaterMark()));

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

                    BookieSideConnectionPeerContextHandler contextHandler =
                        new BookieSideConnectionPeerContextHandler();
                    ChannelPipeline pipeline = ch.pipeline();

                    pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
                    pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));

                    pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.RequestDecoder(registry));
                    pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.ResponseEncoder(registry));
                    pipeline.addLast("bookieAuthHandler", new AuthHandler.ServerSideHandler(
                                contextHandler.getConnectionPeer(), authProviderFactory));

                    ChannelInboundHandler requestHandler = isRunning.get()
                            ? new BookieRequestHandler(conf, requestProcessor, allChannels)
                            : new RejectRequestHandler();
                    pipeline.addLast("bookieRequestHandler", requestHandler);

                    pipeline.addLast("contextHandler", contextHandler);
                }
            });

            // use the same address 'name', so clients can find local Bookie still discovering them using ZK
            jvmBootstrap.bind(bookieAddress.getLocalAddress()).sync();
            LocalBookiesRegistry.registerLocalBookieAddress(bookieAddress);
        }
    }

    void start() throws InterruptedException {
        isRunning.set(true);
    }

    void shutdown() {
        LOG.info("Shutting down BookieNettyServer");
        isRunning.set(false);

        if (!isClosed.compareAndSet(false, true)) {
            // the netty server is already closed.
            return;
        }

        allChannels.close().awaitUninterruptibly();

        if (eventLoopGroup != null) {
            try {
                eventLoopGroup.shutdownGracefully(0, 10, TimeUnit.MILLISECONDS).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                /// OK
            }
        }
        if (jvmEventLoopGroup != null) {
            LocalBookiesRegistry.unregisterLocalBookieAddress(bookieAddress);
            jvmEventLoopGroup.shutdownGracefully();
        }

        authProviderFactory.close();
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
