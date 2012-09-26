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
package org.apache.hedwig.server.regions;

import org.apache.commons.configuration.ConfigurationException;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HedwigHubClientFactory {

    private final ServerConfiguration cfg;
    private final ClientConfiguration clientConfiguration;
    private final ClientSocketChannelFactory channelFactory;
    private static final Logger logger = LoggerFactory.getLogger(HedwigHubClientFactory.class);

    // Constructor that takes in a ServerConfiguration, ClientConfiguration and a ChannelFactory
    // so we can reuse it for all Clients created here.
    public HedwigHubClientFactory(ServerConfiguration cfg, ClientConfiguration clientConfiguration,
                                  ClientSocketChannelFactory channelFactory) {
        this.cfg = cfg;
        this.clientConfiguration = clientConfiguration;
        this.channelFactory = channelFactory;
    }

    /**
     * Manufacture a hub client whose default server to connect to is the input
     * HedwigSocketAddress hub.
     *
     * @param hub
     *            The hub in another region to connect to.
     */
    HedwigHubClient create(final HedwigSocketAddress hub) {
        // Create a hub specific version of the client to use
        ClientConfiguration hubClientConfiguration = new ClientConfiguration() {
            @Override
            protected HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
                return hub;
            }

            @Override
            public boolean isSSLEnabled() {
                return cfg.isInterRegionSSLEnabled() || clientConfiguration.isSSLEnabled();
            }
        };
        try {
            hubClientConfiguration.addConf(this.clientConfiguration.getConf());
        } catch (ConfigurationException e) {
            String msg = "Configuration exception while loading the client configuration for the region manager.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }
        return new HedwigHubClient(hubClientConfiguration, channelFactory);
    }
}
