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

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestCallbacks.AddCallbackFuture;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the bookkeeper client while losing a ZK session.
 */
public class BookKeeperClientZKSessionExpiry extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperClientZKSessionExpiry.class);

    public BookKeeperClientZKSessionExpiry() {
        super(4);
    }

    @Test
    public void testSessionLossWhileWriting() throws Exception {

        Thread expiryThread = new Thread() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            Thread.sleep(5000);
                            long sessionId = bkc.getZkHandle().getSessionId();
                            byte[] sessionPasswd = bkc.getZkHandle().getSessionPasswd();

                            try {
                                ZooKeeperWatcherBase watcher = new ZooKeeperWatcherBase(10000, false);
                                ZooKeeper zk = new ZooKeeper(zkUtil.getZooKeeperConnectString(), 10000,
                                                             watcher, sessionId, sessionPasswd);
                                zk.close();
                            } catch (Exception e) {
                                LOG.info("Error killing session", e);
                            }
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            };
        expiryThread.start();

        for (int i = 0; i < 3; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, 2, BookKeeper.DigestType.MAC, "foobar".getBytes());
            for (int j = 0; j < 100; j++) {
                lh.asyncAddEntry("foobar".getBytes(), new AddCallbackFuture(j), null);
            }
            startNewBookie();
            killBookie(0);

            lh.addEntry("lastEntry".getBytes());

            lh.close();
        }
    }
}
