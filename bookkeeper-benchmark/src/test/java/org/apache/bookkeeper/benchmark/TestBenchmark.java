/*
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
package org.apache.bookkeeper.benchmark;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestBenchmark extends BookKeeperClusterTestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(TestBenchmark.class);

    public TestBenchmark() {
        super(5);
    }

    @Before
    public void setUp() throws Exception {
        baseConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        baseClientConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        super.setUp();
    }

    @Test
    public void testThroughputLatency() throws Exception {
        String latencyFile = System.getProperty("test.latency.file", "latencyDump.dat");
        BenchThroughputLatency.main(new String[] {
                "--zookeeper", zkUtil.getZooKeeperConnectString(),
                "--time", "10",
                "--skipwarmup",
                "--throttle", "1",
                "--sendlimit", "10000",
                "--latencyFile", latencyFile
            });
    }

    @Test
    public void testBookie() throws Exception {
        BookieSocketAddress bookie = getBookie(0);
        BenchBookie.main(new String[] {
                "--host", bookie.getSocketAddress().getHostName(),
                "--port", String.valueOf(bookie.getPort()),
                "--zookeeper", zkUtil.getZooKeeperConnectString(),
                "--warmupCount", "10",
                "--latencyCount", "100",
                "--throughputCount", "100"
                });
    }

    @Test
    public void testReadThroughputLatency() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("read-throughput-latency-test").build());
        Future<Integer> f = executor.submit(() -> {
                BenchReadThroughputLatency.main(new String[] {
                        "--zookeeper", zkUtil.getZooKeeperConnectString(),
                        "--listen", "10"});
                return 0;
            });

        Thread.sleep(2000);
        byte data[] = new byte[1024];
        Arrays.fill(data, (byte)'x');

        long lastLedgerId = 0;
        for (int i = 0; i < 10; i++) {
            BookKeeper bk = new BookKeeper(zkUtil.getZooKeeperConnectString());
            LedgerHandle lh = bk.createLedger(BookKeeper.DigestType.CRC32, "benchPasswd".getBytes());
            lastLedgerId = lh.getId();
            try {
                for (int j = 0; j < 100; j++) {
                    lh.addEntry(data);
                }
            } finally {
                lh.close();
                bk.close();
            }
        }
        Assert.assertEquals(Integer.valueOf(0), f.get());

        BenchReadThroughputLatency.main(new String[] {
                "--zookeeper", zkUtil.getZooKeeperConnectString(),
                "--ledger", String.valueOf(lastLedgerId)});
    }
}
