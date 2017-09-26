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
package org.apache.bookkeeper.client;

import com.google.common.base.Preconditions;
import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import java.io.IOException;
import java.io.UncheckedIOException;
import static org.apache.bookkeeper.client.BKException.Code.ZKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.BookKeeperBuilder;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * Internal builder for {@link org.apache.bookkeeper.client.api.BookKeeper} client
 */
public class BookKeeperBuilderImpl implements BookKeeperBuilder {

    private final ClientConfiguration conf;
    private ZooKeeper zk = null;
    private EventLoopGroup eventLoopGroup;
    private StatsLogger statsLogger = NullStatsLogger.INSTANCE;
    private DNSToSwitchMapping dnsResolver;
    private HashedWheelTimer requestTimer;
    private FeatureProvider featureProvider;

    public BookKeeperBuilderImpl(ClientConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public BookKeeperBuilder with(Object component) {
        Preconditions.checkNotNull(component);
        if (component instanceof ZooKeeper) {
            this.zk = (ZooKeeper) component;
        } else if (component instanceof EventLoopGroup) {
            this.eventLoopGroup = (EventLoopGroup) component;
        } else if (component instanceof StatsLogger) {
            this.statsLogger = (StatsLogger) component;
        } else if (component instanceof DNSToSwitchMapping) {
            this.dnsResolver = (DNSToSwitchMapping) component;
        } else if (component instanceof HashedWheelTimer) {
            this.requestTimer = (HashedWheelTimer) component;
        } else if (component instanceof FeatureProvider) {
            this.featureProvider = (FeatureProvider) component;
        } else {
            throw new IllegalArgumentException("Component of type " + component.getClass().getName()
                    + " is not configurable");
        }
        return this;
    }

    @Override
    public BookKeeper build() throws BKException, InterruptedException {
        try {
            return new org.apache.bookkeeper.client.BookKeeper(conf, zk, eventLoopGroup,
                    statsLogger, dnsResolver, requestTimer, featureProvider);
        }
        catch (KeeperException err) {
            BKException.ZKException zkErr = new BKException.ZKException();
            zkErr.initCause(err);
            throw zkErr;
        }
        catch (IOException err) {
            throw new UncheckedIOException(err);
        }
    }

}
