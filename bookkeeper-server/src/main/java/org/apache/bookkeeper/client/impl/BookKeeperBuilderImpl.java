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
package org.apache.bookkeeper.client.impl;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import java.io.IOException;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.BookKeeperBuilder;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Internal builder for {@link org.apache.bookkeeper.client.api.BookKeeper} client.
 *
 * @since 4.6
 */
public class BookKeeperBuilderImpl implements BookKeeperBuilder {

    private final org.apache.bookkeeper.client.BookKeeper.Builder builder;

    public BookKeeperBuilderImpl(ClientConfiguration conf) {
        this.builder = org.apache.bookkeeper.client.BookKeeper.forConfig(conf);
    }

    @Override
    public BookKeeperBuilder eventLoopGroup(EventLoopGroup eventLoopGroup) {
        builder.eventLoopGroup(eventLoopGroup);
        return this;
    }

    @Override
    public BookKeeperBuilder allocator(ByteBufAllocator allocator) {
        builder.allocator(allocator);
        return this;
    }

    @Override
    public BookKeeperBuilder statsLogger(StatsLogger statsLogger) {
        builder.statsLogger(statsLogger);
        return this;
    }

    @Override
    public BookKeeperBuilder dnsResolver(DNSToSwitchMapping dnsResolver) {
        builder.dnsResolver(dnsResolver);
        return this;
    }

    @Override
    public BookKeeperBuilder requestTimer(HashedWheelTimer requeestTimer) {
        builder.requestTimer(requeestTimer);
        return this;
    }

    @Override
    public BookKeeperBuilder featureProvider(FeatureProvider featureProvider) {
        builder.featureProvider(featureProvider);
        return this;
    }

    @Override
    public BookKeeper build() throws InterruptedException, BKException, IOException  {
        return builder.build();
    }

}
