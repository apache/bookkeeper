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
package org.apache.hedwig.client;

import org.apache.hedwig.client.api.Client;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.netty.HedwigClientImpl;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.jboss.netty.channel.ChannelFactory;

/**
 * Hedwig client uses as starting point for all communications with the Hedwig service.
 * 
 * @see Publisher
 * @see Subscriber
 */
public class HedwigClient implements Client {
    private final Client impl;

    /**
     * Construct a hedwig client object. The configuration object
     * should be an instance of a class which implements ClientConfiguration.
     *
     * @param cfg The client configuration.
     */
    public HedwigClient(ClientConfiguration cfg) {
        impl = HedwigClientImpl.create(cfg);
    }

    /**
     * Construct a hedwig client object, using a preexisting socket factory.
     * This is useful if you need to create many hedwig client instances.
     *
     * @param cfg The client configuration
     * @param socketFactory A netty socket factory.
     */
    public HedwigClient(ClientConfiguration cfg, ChannelFactory socketFactory) {
        impl = HedwigClientImpl.create(cfg, socketFactory);
    }

    @Override
    public Publisher getPublisher() {
        return impl.getPublisher();
    }

    @Override
    public Subscriber getSubscriber() {
        return impl.getSubscriber();
    }

    @Override
    public void close() {
        impl.close();
    }
}