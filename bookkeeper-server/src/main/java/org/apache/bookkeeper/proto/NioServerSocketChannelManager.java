/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.proto;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * Manages a NioServerSocketChannel channel
 *
 * @author enrico.olivelli
 */
public class NioServerSocketChannelManager extends ChannelManager {

    private ChannelFactory channelFactory;

    @Override
    public Channel start(ServerConfiguration conf, ChannelPipelineFactory bookiePipelineFactory) throws IOException {
        BookieSocketAddress bookieAddress = Bookie.getBookieAddress(conf);
        ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
        String base = "bookie-" + conf.getBookiePort() + "-netty";
        this.channelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(tfb.setNameFormat(base + "-boss-%d").build()),
                Executors.newCachedThreadPool(tfb.setNameFormat(base + "-worker-%d").build()));

        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
        bootstrap.setPipelineFactory(bookiePipelineFactory);
        bootstrap.setOption("child.tcpNoDelay", conf.getServerTcpNoDelay());
        bootstrap.setOption("child.soLinger", 2);

        InetSocketAddress bindAddress;
        if (conf.getListeningInterface() == null) {
            // listen on all interfaces
            bindAddress = new InetSocketAddress(conf.getBookiePort());
        } else {
            bindAddress = bookieAddress.getSocketAddress();
        }
        
        Channel listen = bootstrap.bind(bindAddress);
        return listen;
    }

    @Override
    public void close() {
        if (channelFactory != null) {
            channelFactory.releaseExternalResources();
        }
        channelFactory = null;
    }
}