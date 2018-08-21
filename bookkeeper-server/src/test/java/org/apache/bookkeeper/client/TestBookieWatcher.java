/*
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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

/**
 * Test a bookie watcher.
 */
public class TestBookieWatcher extends BookKeeperClusterTestCase {

    public TestBookieWatcher() {
        super(2);
    }

    private void expireZooKeeperSession(ZooKeeper zk, int timeout)
            throws IOException, InterruptedException, KeeperException {
        final CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper newZk = new ZooKeeper(zkUtil.getZooKeeperConnectString(), timeout,
                new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.None && event.getState() == KeeperState.SyncConnected) {
                    latch.countDown();
                }
            }

        }, zk.getSessionId(), zk.getSessionPasswd());
        if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
            throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
        }
        newZk.close();
    }

    @Test
    public void testBookieWatcherSurviveWhenSessionExpired() throws Exception {
        final int timeout = 2000;
        try (ZooKeeperClient zk = ZooKeeperClient.newBuilder()
                .connectString(zkUtil.getZooKeeperConnectString())
                .sessionTimeoutMs(timeout)
                .build()) {
            runBookieWatcherWhenSessionExpired(zk, timeout, true);
        }
    }

    @Test
    public void testBookieWatcherDieWhenSessionExpired() throws Exception {
        final int timeout = 2000;
        final CountDownLatch connectLatch = new CountDownLatch(1);

        @Cleanup
        ZooKeeper zk = new ZooKeeper(zkUtil.getZooKeeperConnectString(), timeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (EventType.None == watchedEvent.getType()
                        && KeeperState.SyncConnected == watchedEvent.getState()) {
                    connectLatch.countDown();
                }
            }
        });

        connectLatch.await();
        runBookieWatcherWhenSessionExpired(zk, timeout, false);
    }

    private void runBookieWatcherWhenSessionExpired(ZooKeeper zk, int timeout, boolean reconnectable)
            throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(metadataServiceUri);

        try (BookKeeper bkc = new BookKeeper(conf, zk)) {

            LedgerHandle lh;
            try {
                lh = bkc.createLedger(3, 2, 2, BookKeeper.DigestType.CRC32, new byte[]{});
                fail("Should fail to create ledger due to not enough bookies.");
            } catch (BKException bke) {
                // expected
            }

            // make zookeeper session expired
            expireZooKeeperSession(bkc.getZkHandle(), timeout);
            TimeUnit.MILLISECONDS.sleep(3 * timeout);

            // start four new bookies
            for (int i = 0; i < 2; i++) {
                startNewBookie();
            }

            // wait for bookie watcher backoff time.
            TimeUnit.SECONDS.sleep(1);

            // should success to detect newly added bookies
            try {
                lh = bkc.createLedger(3, 2, 2, BookKeeper.DigestType.CRC32, new byte[]{});
                lh.close();
                if (!reconnectable) {
                    fail("Should fail to create ledger due to bookie watcher could not survive after session expire.");
                }
            } catch (BKException bke) {
                if (reconnectable) {
                    fail("Should not fail to create ledger due to bookie watcher could survive after session expire.");
                }
            }
        }
    }
}
