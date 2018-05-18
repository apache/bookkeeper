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

package org.apache.bookkeeper.tests.integration.cluster;

import com.google.common.base.Stopwatch;
import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.tests.integration.topologies.BKCluster;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * A docker container based bookkeeper cluster test base, providing similar facility like the one in unit test.
 */
@Slf4j
public abstract class BookKeeperClusterTestBase {

    protected static BKCluster bkCluster;
    protected static URI metadataServiceUri;
    protected static MetadataClientDriver metadataClientDriver;
    protected static ScheduledExecutorService executor;

    @BeforeClass
    public static void setupCluster() throws Exception {
        bkCluster = new BKCluster(RandomStringUtils.randomAlphabetic(8), 0);
        bkCluster.start();

        metadataServiceUri = URI.create(bkCluster.getExternalServiceUri());
        ClientConfiguration conf = new ClientConfiguration()
            .setMetadataServiceUri(metadataServiceUri.toString());
        executor = Executors.newSingleThreadScheduledExecutor();
        metadataClientDriver = MetadataDrivers.getClientDriver(metadataServiceUri);
        metadataClientDriver.initialize(conf, executor, NullStatsLogger.INSTANCE, Optional.empty());
    }

    @AfterClass
    public static void teardownCluster() {
        if (null != metadataClientDriver) {
            metadataClientDriver.close();
        }
        if (null != executor) {
            executor.shutdown();
        }
        if (null != bkCluster) {
            bkCluster.stop();
        }
    }

    private boolean findIfBookieRegistered(String bookieName) throws Exception {
        Set<BookieSocketAddress> bookies =
            FutureUtils.result(metadataClientDriver.getRegistrationClient().getWritableBookies()).getValue();
        Optional<BookieSocketAddress> registered =
            bookies.stream().filter(addr -> addr.getHostName().equals(bookieName)).findFirst();
        return registered.isPresent();
    }

    protected void waitUntilBookieUnregistered(String bookieName) throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        while (findIfBookieRegistered(bookieName)) {
            TimeUnit.MILLISECONDS.sleep(1000);
            log.info("Bookie {} is still registered in cluster {} after {} ms elapsed",
                bookieName, bkCluster.getClusterName(), sw.elapsed(TimeUnit.MILLISECONDS));
        }
    }

}
