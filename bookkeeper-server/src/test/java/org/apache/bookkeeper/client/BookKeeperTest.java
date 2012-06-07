package org.apache.bookkeeper.client;

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

import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests of the main BookKeeper client
 */
public class BookKeeperTest extends BookKeeperClusterTestCase {
    public BookKeeperTest() {
        super(4);
    }

    @Test
    public void testConstructionZkDelay() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(20000);

        CountDownLatch l = new CountDownLatch(1);
        zkUtil.sleepServer(5, l);
        l.await();

        BookKeeper bkc = new BookKeeper(conf);
        bkc.createLedger(DigestType.CRC32, "testPasswd".getBytes()).close();
        bkc.close();
    }

    @Test
    public void testConstructionNotConnectedExplicitZk() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(20000);

        CountDownLatch l = new CountDownLatch(1);
        zkUtil.sleepServer(5, l);
        l.await();

        ZooKeeper zk = new ZooKeeper(zkUtil.getZooKeeperConnectString(), 10000,
                            new Watcher() {
                                @Override
                                public void process(WatchedEvent event) {
                                }
                            });
        assertFalse("ZK shouldn't have connected yet", zk.getState().isConnected());
        try {
            BookKeeper bkc = new BookKeeper(conf, zk);
            fail("Shouldn't be able to construct with unconnected zk");
        } catch (KeeperException.ConnectionLossException cle) {
            // correct behaviour
        }
    }
}