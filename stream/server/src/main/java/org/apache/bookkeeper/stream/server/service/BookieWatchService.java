/*
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

package org.apache.bookkeeper.stream.server.service;

import com.google.common.base.Stopwatch;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.server.conf.BookieConfiguration;

/**
 * A service that watches bookies and wait for minimum number of bookies to be alive.
 */
@Slf4j
public class BookieWatchService
    extends AbstractLifecycleComponent<BookieConfiguration> {

    private final int minNumBookies;

    public BookieWatchService(int minNumBookies,
                              BookieConfiguration conf,
                              StatsLogger statsLogger) {
        super("bookie-watcher", conf, statsLogger);
        this.minNumBookies = minNumBookies;
    }

    @Override
    protected void doStart() {
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.loadConf(conf.getUnderlyingConf());

        @Cleanup("shutdown") ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        try {
            MetadataDrivers.runFunctionWithMetadataClientDriver(clientConf, clientDriver -> {
                try {
                    waitingForNumBookies(clientDriver.getRegistrationClient(), minNumBookies);
                } catch (Exception e) {
                    log.error("Encountered exceptions on waiting {} bookies to be alive", minNumBookies);
                    throw new RuntimeException("Encountered exceptions on waiting "
                        + minNumBookies + " bookies to be alive", e);
                }
                return (Void) null;
            }, executorService);
        } catch (MetadataException | ExecutionException  e) {
            throw new RuntimeException("Failed to start bookie watch service", e);
        }
    }

    private static void waitingForNumBookies(RegistrationClient client, int minNumBookies) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Set<BookieSocketAddress> bookies = FutureUtils.result(client.getWritableBookies()).getValue();
        while (bookies.size() < minNumBookies) {
            TimeUnit.SECONDS.sleep(1);
            bookies = FutureUtils.result(client.getWritableBookies()).getValue();
            log.info("Only {} bookies are live since {} seconds elapsed, "
                + "wait for another {} bookies for another 1 second",
                bookies.size(), stopwatch.elapsed(TimeUnit.SECONDS), minNumBookies - bookies.size());
        }
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}

}
