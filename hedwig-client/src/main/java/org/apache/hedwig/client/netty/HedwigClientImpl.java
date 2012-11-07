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
package org.apache.hedwig.client.netty;

import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.google.protobuf.ByteString;

import org.apache.hedwig.client.api.Client;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.netty.impl.simple.SimpleHChannelManager;
import org.apache.hedwig.client.netty.impl.multiplex.MultiplexHChannelManager;

/**
 * This is a top level Hedwig Client class that encapsulates the common
 * functionality needed for both Publish and Subscribe operations.
 *
 */
public class HedwigClientImpl implements Client {

    private static final Logger logger = LoggerFactory.getLogger(HedwigClientImpl.class);

    // The Netty socket factory for making connections to the server.
    protected final ChannelFactory socketFactory;
    // Whether the socket factory is one we created or is owned by whoever
    // instantiated us.
    protected boolean ownChannelFactory = false;

    // channel manager manages all the channels established by the client
    protected final HChannelManager channelManager;

    private HedwigSubscriber sub;
    private final HedwigPublisher pub;
    private final ClientConfiguration cfg;

    public static Client create(ClientConfiguration cfg) {
        return new HedwigClientImpl(cfg);
    }

    public static Client create(ClientConfiguration cfg, ChannelFactory socketFactory) {
        return new HedwigClientImpl(cfg, socketFactory);
    }

    // Base constructor that takes in a Configuration object.
    // This will create its own client socket channel factory.
    protected HedwigClientImpl(ClientConfiguration cfg) {
        this(cfg, new NioClientSocketChannelFactory(
                  Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        ownChannelFactory = true;
    }

    // Constructor that takes in a Configuration object and a ChannelFactory
    // that has already been instantiated by the caller.
    protected HedwigClientImpl(ClientConfiguration cfg, ChannelFactory socketFactory) {
        this.cfg = cfg;
        this.socketFactory = socketFactory;
        if (cfg.isSubscriptionChannelSharingEnabled()) {
            channelManager = new MultiplexHChannelManager(cfg, socketFactory);
        } else {
            channelManager = new SimpleHChannelManager(cfg, socketFactory);
        }
        pub = new HedwigPublisher(this);
        sub = new HedwigSubscriber(this);
    }

    public ClientConfiguration getConfiguration() {
        return cfg;
    }

    public HChannelManager getHChannelManager() {
        return channelManager;
    }

    public HedwigSubscriber getSubscriber() {
        return sub;
    }

    // Protected method to set the subscriber. This is needed currently for hub
    // versions of the client subscriber.
    protected void setSubscriber(HedwigSubscriber sub) {
        this.sub = sub;
    }

    public HedwigPublisher getPublisher() {
        return pub;
    }

    // When we are done with the client, this is a clean way to gracefully close
    // all channels/sockets created by the client and to also release all
    // resources used by netty.
    public void close() {
        logger.info("Stopping the client!");

        // close channel manager to release all channels
        channelManager.close(); 

        // Release resources used by the ChannelFactory on the client if we are
        // the owner that created it.
        if (ownChannelFactory) {
            socketFactory.releaseExternalResources();
        }
        logger.info("Completed stopping the client!");
    }

}
