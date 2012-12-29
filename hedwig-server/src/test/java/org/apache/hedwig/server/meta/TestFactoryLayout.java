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

package org.apache.hedwig.server.meta;

import java.io.IOException;

import org.apache.hedwig.protocol.PubSubProtocol.ManagerMeta;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.zookeeper.ZooKeeperTestBase;
import org.apache.hedwig.zookeeper.ZkUtils;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import org.junit.Test;
import org.junit.Assert;

public class TestFactoryLayout extends ZooKeeperTestBase {

    @Test(timeout=60000)
    public void testFactoryLayout() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setMetadataManagerFactoryName(
            "org.apache.hedwig.server.meta.ZkMetadataManager");

        FactoryLayout layout = FactoryLayout.readLayout(zk, conf);
        Assert.assertTrue("Layout should be null", layout == null);

        String testName = "foobar";
        int testVersion = 0xdeadbeef;
        // use layout defined in configuration also create it in zookeeper
        writeFactoryLayout(conf, testName, testVersion);

        layout = FactoryLayout.readLayout(zk, conf);
        Assert.assertEquals(testName, layout.getManagerMeta().getManagerImpl());
        Assert.assertEquals(testVersion, layout.getManagerMeta().getManagerVersion());
    }

    private void writeFactoryLayout(ServerConfiguration conf, String managerCls,
                                    int managerVersion)
        throws Exception {
        ManagerMeta managerMeta = ManagerMeta.newBuilder()
                                             .setManagerImpl(managerCls)
                                             .setManagerVersion(managerVersion)
                                             .build();
        FactoryLayout layout = new FactoryLayout(managerMeta);
        layout.store(zk, conf);
    }

    @Test(timeout=60000)
    public void testCorruptedFactoryLayout() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        StringBuilder msb = new StringBuilder();
        String factoryLayoutPath = FactoryLayout.getFactoryLayoutPath(msb, conf);
        // write corrupted manager layout
        ZkUtils.createFullPathOptimistic(zk, factoryLayoutPath, "BadLayout".getBytes(),
                                         Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        try {
            FactoryLayout.readLayout(zk, conf);
            Assert.fail("Shouldn't reach here!");
        } catch (IOException ie) {
        }
    }
}
