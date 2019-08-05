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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test benchmarks.
 */
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

    @Override
    protected String getMetadataServiceUri(String ledgersRootPath) {
        return zkUtil.getMetadataServiceUri(ledgersRootPath, "flat");
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
        final AtomicBoolean threwException = new AtomicBoolean(false);
        Thread t = new Thread() {
                public void run() {
                    try {
                        BenchReadThroughputLatency.main(new String[] {
                                "--zookeeper", zkUtil.getZooKeeperConnectString(),
                                "--listen", "10"});
                    } catch (Throwable t) {
                        LOG.error("Error reading", t);
                        threwException.set(true);
                    }
                }
            };
        t.start();

        Thread.sleep(10000);
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 'x');

        long lastLedgerId = 0;
        Assert.assertTrue("Thread should be running", t.isAlive());
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
        for (int i = 0; i < 60; i++) {
            if (!t.isAlive()) {
                break;
            }
            Thread.sleep(100);
        }

        Assert.assertFalse("Thread should be finished", t.isAlive());

        BenchReadThroughputLatency.main(new String[] {
                "--zookeeper", zkUtil.getZooKeeperConnectString(),
                "--ledger", String.valueOf(lastLedgerId)});
    }
}
