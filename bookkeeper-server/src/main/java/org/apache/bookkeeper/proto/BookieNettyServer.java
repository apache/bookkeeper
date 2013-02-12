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
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.zookeeper.KeeperException;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty server for serving bookie requests
 */
class BookieNettyServer {
    static Logger LOG = LoggerFactory.getLogger(BookieNettyServer.class);

    final static int maxMessageSize = 0xfffff;
    final ServerConfiguration conf;
    final ChannelFactory serverChannelFactory;
    final Bookie bookie;
    final ChannelGroup allChannels = new CleanupChannelGroup();
    final AtomicBoolean isRunning = new AtomicBoolean(false);
    Object suspensionLock = new Object();
    boolean suspended = false;

    InetSocketAddress localAddress = null;

    BookieNettyServer(ServerConfiguration conf, Bookie bookie)
            throws IOException, KeeperException, InterruptedException, BookieException  {
        this.conf = conf;
        this.bookie = bookie;

        ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
        String base = "bookie-" + conf.getBookiePort() + "-netty";
        serverChannelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(tfb.setNameFormat(base + "-boss-%d").build()),
                Executors.newCachedThreadPool(tfb.setNameFormat(base + "-worker-%d").build()));

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

    InetSocketAddress getLocalAddress() {
        if (localAddress != null) {
            return localAddress;
        } else {
            return new InetSocketAddress(conf.getBookiePort());
        }
    }

    void start() {
        ServerBootstrap bootstrap = new ServerBootstrap(serverChannelFactory);
        bootstrap.setPipelineFactory(new BookiePipelineFactory());
        bootstrap.setOption("child.tcpNoDelay", conf.getServerTcpNoDelay());
        bootstrap.setOption("child.soLinger", 2);

        Channel listen = bootstrap.bind(new InetSocketAddress(conf.getBookiePort()));

        assert(listen.getLocalAddress() instanceof InetSocketAddress);
        localAddress = (InetSocketAddress)listen.getLocalAddress();
        allChannels.add(listen);
        isRunning.set(true);
    }

    void shutdown() {
        isRunning.set(false);
        allChannels.close().awaitUninterruptibly();
        serverChannelFactory.releaseExternalResources();
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

            pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.Decoder());
            pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.Encoder());
            pipeline.addLast("bookieRequestHandler", new BookieRequestHandler(conf, bookie,
                                                                              allChannels));
            return pipeline;
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
