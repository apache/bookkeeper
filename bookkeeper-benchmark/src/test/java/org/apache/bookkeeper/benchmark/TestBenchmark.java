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

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Assert;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class TestBenchmark {
    protected static final Logger LOG = LoggerFactory.getLogger(TestBenchmark.class);

    private static Thread ensembleThread = null;
    private final static String zkString = "localhost:2181";
    private static List<String> bookies = null;

    @BeforeClass
    public static void startEnsemble() throws Exception {
        final int numBookies = 5;

        ensembleThread = new Thread() {
                public void run() {
                    try {
                        LocalBookKeeper.main(new String[]{String.valueOf(numBookies)});
                    } catch (InterruptedException ie) {
                        LOG.info("Shutting down ensemble thread");
                    } catch (Exception e) {
                        LOG.error("Error running bookkeeper ensemble", e);
                    }
                }
            };
        ensembleThread.start();

        if (!LocalBookKeeper.waitForServerUp(zkString, 5000)) {
            throw new Exception("Failed to start zookeeper");
        }
        ZooKeeper zk = null;
        try {
            final CountDownLatch connectLatch = new CountDownLatch(1);

            zk = new ZooKeeper(zkString, 15000, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getState() == KeeperState.SyncConnected) {
                            connectLatch.countDown();
                        }
                    }});
            if (!connectLatch.await(10, TimeUnit.SECONDS)) {
                LOG.error("Couldn't connect to zookeeper at " + zkString);
            } else {
                for (int i = 0; i < 10; i++) {
                    try {
                        bookies = zk.getChildren("/ledgers/available", false);
                        if (zk.getChildren("/ledgers/available", false).size()
                            == numBookies) {
                            return;
                        }
                    } catch (Exception e) {
                        // do nothing
                    }
                    Thread.sleep(1000);
                }
                throw new Exception("Not enough bookies started");
            }
        } finally {
            zk.close();
        }
    }

    @AfterClass
    public static void stopEnsemble() throws Exception {
        if (ensembleThread != null) {
            ensembleThread.interrupt();
            ensembleThread.join();
        }
    }

    @Test
    public void testThroughputLatency() throws Exception {
        String latencyFile = System.getProperty("test.latency.file", "latencyDump.dat");
        BenchThroughputLatency.main(new String[] {
                "--time", "10",
                "--skipwarmup",
                "--throttle", "1",
                "--sendlimit", "10000",
                "--latencyFile", latencyFile
            });
    }

    @Test
    public void testBookie() throws Exception {
        String bookie = bookies.get(0);
        String[] parts = bookie.split(":");
        BenchBookie.main(new String[] {
                "--host", parts[0],
                "--port", parts[1],
                "--zookeeper", zkString
                });
    }

    @Test
    public void testReadThroughputLatency() throws Exception {
        AtomicBoolean threwException = new AtomicBoolean(false);
        Thread t = new Thread() {
                public void run() {
                    try {
                        BenchReadThroughputLatency.main(new String[] {
                                "--listen", "10"});
                    } catch (Throwable t) {
                        LOG.error("Error reading", t);
                    }
                }
            };
        t.start();

        Thread.sleep(10000);
        byte data[] = new byte[1024];
        Arrays.fill(data, (byte)'x');

        long lastLedgerId = 0;
        Assert.assertTrue("Thread should be running", t.isAlive());
        for (int i = 0; i < 10; i++) {
            BookKeeper bk = new BookKeeper(zkString);
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
            Thread.sleep(1000); // wait for 10 seconds for reading to finish
        }

        Assert.assertFalse("Thread should be finished", t.isAlive());

        BenchReadThroughputLatency.main(new String[] {
                "--ledger", String.valueOf(lastLedgerId)});

        final long nextLedgerId = lastLedgerId+1;
        t = new Thread() {
                public void run() {
                    try {
                        BenchReadThroughputLatency.main(new String[] {
                                "--ledger", String.valueOf(nextLedgerId)});
                    } catch (Throwable t) {
                        LOG.error("Error reading", t);
                    }
                }
            };
        t.start();

        Assert.assertTrue("Thread should be running", t.isAlive());
        BookKeeper bk = new BookKeeper(zkString);
        LedgerHandle lh = bk.createLedger(BookKeeper.DigestType.CRC32, "benchPasswd".getBytes());
        try {
            for (int j = 0; j < 100; j++) {
                lh.addEntry(data);
            }
        } finally {
            lh.close();
            bk.close();
        }
        for (int i = 0; i < 60; i++) {
            if (!t.isAlive()) {
                break;
            }
            Thread.sleep(1000); // wait for 10 seconds for reading to finish
        }
        Assert.assertFalse("Thread should be finished", t.isAlive());
    }
}
