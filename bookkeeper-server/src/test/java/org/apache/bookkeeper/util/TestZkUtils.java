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
package org.apache.bookkeeper.util;

import java.io.IOException;

import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

public class TestZkUtils extends TestCase {

    static final Logger logger = LoggerFactory.getLogger(TestZkUtils.class);

    // ZooKeeper related variables
    protected ZooKeeperUtil zkUtil = new ZooKeeperUtil();

    @Before
    @Override
    public void setUp() throws Exception {
        logger.info("Setting up test {}.", getName());
        zkUtil.startServer();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        zkUtil.killServer();
        logger.info("Teared down test {}.", getName());
    }

    @Test(timeout = 60000)
    public void testAsyncCreateAndDeleteFullPathOptimistic() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zkc = new ZooKeeper(zkUtil.getZooKeeperConnectString(), 10000, null);
        /*
         * "/ledgers/available" is already created in ZooKeeperUtil.startServer
         */
        String ledgerZnodePath = new String("/ledgers/000/000/000/001");
        ZkUtils.createFullPathOptimistic(zkc, ledgerZnodePath, "data".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        assertTrue(ledgerZnodePath + " zNode should exist", null != zkc.exists(ledgerZnodePath, false));

        ledgerZnodePath = new String("/ledgers/000/000/000/002");
        ZkUtils.createFullPathOptimistic(zkc, ledgerZnodePath, "data".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        assertTrue(ledgerZnodePath + " zNode should exist", null != zkc.exists(ledgerZnodePath, false));

        ZkUtils.deleteFullPathOptimistic(zkc, ledgerZnodePath, -1);
        assertTrue(ledgerZnodePath + " zNode should not exist, since it is deleted",
                null == zkc.exists(ledgerZnodePath, false));

        ledgerZnodePath = new String("/ledgers/000/000/000/001");
        assertTrue(ledgerZnodePath + " zNode should exist", null != zkc.exists(ledgerZnodePath, false));
        ZkUtils.deleteFullPathOptimistic(zkc, ledgerZnodePath, -1);
        assertTrue(ledgerZnodePath + " zNode should not exist, since it is deleted",
                null == zkc.exists(ledgerZnodePath, false));
        assertTrue("/ledgers/000" + " zNode should not exist, since it should be deleted recursively",
                null == zkc.exists(ledgerZnodePath, false));
    }
}
