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
package org.apache.bookkeeper.replication;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestCallbacks;

import java.util.List;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerHandleAdapter;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;

import org.apache.bookkeeper.util.ZkUtils;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test verifies that the period check on the auditor
 * will pick up on missing data in the client
 */
public class AuditorPeriodicBookieCheckTest extends BookKeeperClusterTestCase {
    private final static Logger LOG = LoggerFactory
            .getLogger(AuditorPeriodicBookieCheckTest.class);

    private AuditorElector auditorElector = null;
    private ZooKeeper auditorZookeeper = null;

    private final static int CHECK_INTERVAL = 1; // run every second

    public AuditorPeriodicBookieCheckTest() {
        super(3);
        baseConf.setPageLimit(1); // to make it easy to push ledger out of cache
        baseConf.setAllowLoopback(true);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        ServerConfiguration conf = new ServerConfiguration(bsConfs.get(0));
        conf.setAllowLoopback(true);
        conf.setAuditorPeriodicBookieCheckInterval(CHECK_INTERVAL);
        String addr = bs.get(0).getLocalAddress().toString();

        ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(10000);
        auditorZookeeper = ZkUtils.createConnectedZookeeperClient(
                zkUtil.getZooKeeperConnectString(), w);

        auditorElector = new AuditorElector(addr, conf, auditorZookeeper);
        auditorElector.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        auditorElector.shutdown();
        auditorZookeeper.close();

        super.tearDown();
    }

    /**
     * Test that the periodic bookie checker works
     */
    @Test(timeout=30000)
    public void testPeriodicBookieCheckInterval() throws Exception {
        LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bsConfs.get(0), zkc);
        LedgerManager ledgerManager = mFactory.newLedgerManager();
        final LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        final int numLedgers = 1;

        LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
        LedgerMetadata md = LedgerHandleAdapter.getLedgerMetadata(lh);
        List<BookieSocketAddress> ensemble = md.getEnsembles().get(0L);
        ensemble.set(0, new BookieSocketAddress("1.1.1.1", 1000));

        TestCallbacks.GenericCallbackFuture<Void> cb = new TestCallbacks.GenericCallbackFuture<Void>();
        ledgerManager.writeLedgerMetadata(lh.getId(), md, cb);
        cb.get();

        long underReplicatedLedger = -1;
        for (int i = 0; i < 10; i++) {
            underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
            if (underReplicatedLedger != -1) {
                break;
            }
            Thread.sleep(CHECK_INTERVAL*1000);
        }
        assertEquals("Ledger should be under replicated", lh.getId(), underReplicatedLedger);
    }
}
