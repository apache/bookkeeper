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
 */
package org.apache.bookkeeper.discover;

import static org.junit.Assert.assertNotNull;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link RegistrationManager}.
 */
public class TestZkRegistrationManager {

    private ZooKeeperCluster localZkServer;
    private ZooKeeper zkc;

    @Before
    public void setup() throws Exception {
        localZkServer = new ZooKeeperUtil();
        localZkServer.startCluster();
    }

    @After
    public void teardown() throws Exception {
        localZkServer.stopCluster();
    }

    @Test
    public void testPrepareFormat () throws Exception {
        try {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setMetadataServiceUri("zk+hierarchical://localhost:2181/test/ledgers");
            zkc = localZkServer.getZooKeeperClient();
            ZKRegistrationManager zkRegistrationManager = new ZKRegistrationManager(conf, zkc, () -> {});
            zkRegistrationManager.prepareFormat();
            assertNotNull(zkc.exists("/test/ledgers", false));
        } finally {
            if (zkc != null) {
                zkc.close();
            }
        }
    }

}
