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

import java.io.IOException;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;

/**
 * Manages VM-local channels
 *
 * @author enrico.olivelli
 */
public class VMLocalChannelManager extends ChannelManager {

    private ChannelFactory channelFactory;
    private BookieSocketAddress bookieAddress;

    @Override
    public Channel start(ServerConfiguration conf, ChannelPipelineFactory bookiePipelineFactory) throws IOException {
        BookieSocketAddress bookieAddress = Bookie.getBookieAddress(conf);
        this.channelFactory = new DefaultLocalServerChannelFactory();
        this.bookieAddress = bookieAddress;
        ServerBootstrap jvmbootstrap = new ServerBootstrap(channelFactory);
        jvmbootstrap.setPipelineFactory(bookiePipelineFactory);

        // use the same address 'name', so clients can find local Bookie still discovering them using ZK
        Channel jvmlisten = jvmbootstrap.bind(bookieAddress.getLocalAddress());
        LocalBookiesRegistry.registerLocalBookieAddress(bookieAddress);
        return jvmlisten;
    }

    @Override
    public void close() {
        LocalBookiesRegistry.unregisterLocalBookieAddress(bookieAddress);
        if (channelFactory != null) {
            channelFactory.releaseExternalResources();
        }
        channelFactory = null;
    }

}
