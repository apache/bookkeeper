/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.acl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.TestZooKeeperClientBuilder;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.ZooKeeperClusterTestCase;
import org.apache.distributedlog.impl.acl.ZKAccessControl;
import org.apache.distributedlog.thrift.AccessControlEntry;
import org.apache.distributedlog.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;




/**
 * TestZKAccessControl.
 */
public class TestZKAccessControl extends ZooKeeperClusterTestCase {

    private ZooKeeperClient zkc;

    @Before
    public void setup() throws Exception {
        zkc = TestZooKeeperClientBuilder.newBuilder()
                .uri(createURI("/"))
                .build();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    private URI createURI(String path) {
        return URI.create("distributedlog://127.0.0.1:" + zkPort + path);
    }

    @Test(timeout = 60000)
    public void testCreateZKAccessControl() throws Exception {
        AccessControlEntry ace = new AccessControlEntry();
        ace.setDenyWrite(true);
        String zkPath = "/create-zk-access-control";
        ZKAccessControl zkac = new ZKAccessControl(ace, zkPath);
        Utils.ioResult(zkac.create(zkc));

        ZKAccessControl readZKAC = Utils.ioResult(ZKAccessControl.read(zkc, zkPath, null));
        assertEquals(zkac, readZKAC);

        ZKAccessControl another = new ZKAccessControl(ace, zkPath);
        try {
            FutureUtils.result(another.create(zkc));
        } catch (KeeperException.NodeExistsException ke) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testDeleteZKAccessControl() throws Exception {
        String zkPath = "/delete-zk-access-control";

        AccessControlEntry ace = new AccessControlEntry();
        ace.setDenyDelete(true);

        ZKAccessControl zkac = new ZKAccessControl(ace, zkPath);
        Utils.ioResult(zkac.create(zkc));

        ZKAccessControl readZKAC = Utils.ioResult(ZKAccessControl.read(zkc, zkPath, null));
        assertEquals(zkac, readZKAC);

        Utils.ioResult(ZKAccessControl.delete(zkc, zkPath));

        try {
            FutureUtils.result(ZKAccessControl.read(zkc, zkPath, null));
        } catch (KeeperException.NoNodeException nne) {
            // expected.
        }
        Utils.ioResult(ZKAccessControl.delete(zkc, zkPath));
    }

    @Test(timeout = 60000)
    public void testEmptyZKAccessControl() throws Exception {
        String zkPath = "/empty-access-control";

        zkc.get().create(zkPath, new byte[0], zkc.getDefaultACL(), CreateMode.PERSISTENT);

        ZKAccessControl readZKAC = Utils.ioResult(ZKAccessControl.read(zkc, zkPath, null));

        assertEquals(zkPath, readZKAC.getZKPath());
        assertEquals(ZKAccessControl.DEFAULT_ACCESS_CONTROL_ENTRY, readZKAC.getAccessControlEntry());
        assertTrue(ZKAccessControl.DEFAULT_ACCESS_CONTROL_ENTRY == readZKAC.getAccessControlEntry());
    }

    @Test(timeout = 60000)
    public void testCorruptedZKAccessControl() throws Exception {
        String zkPath = "/corrupted-zk-access-control";

        zkc.get().create(zkPath, "corrupted-data".getBytes(UTF_8), zkc.getDefaultACL(), CreateMode.PERSISTENT);

        try {
            Utils.ioResult(ZKAccessControl.read(zkc, zkPath, null));
        } catch (ZKAccessControl.CorruptedAccessControlException cace) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testUpdateZKAccessControl() throws Exception {
        String zkPath = "/update-zk-access-control";

        AccessControlEntry ace = new AccessControlEntry();
        ace.setDenyDelete(true);

        ZKAccessControl zkac = new ZKAccessControl(ace, zkPath);
        Utils.ioResult(zkac.create(zkc));

        ZKAccessControl readZKAC = Utils.ioResult(ZKAccessControl.read(zkc, zkPath, null));
        assertEquals(zkac, readZKAC);

        ace.setDenyRelease(true);
        ZKAccessControl newZKAC = new ZKAccessControl(ace, zkPath);
        Utils.ioResult(newZKAC.update(zkc));
        ZKAccessControl readZKAC2 = Utils.ioResult(ZKAccessControl.read(zkc, zkPath, null));
        assertEquals(newZKAC, readZKAC2);

        try {
            FutureUtils.result(readZKAC.update(zkc));
        } catch (KeeperException.BadVersionException bve) {
            // expected
        }
        readZKAC2.getAccessControlEntry().setDenyTruncate(true);
        Utils.ioResult(readZKAC2.update(zkc));
        ZKAccessControl readZKAC3 = Utils.ioResult(ZKAccessControl.read(zkc, zkPath, null));
        assertEquals(readZKAC2, readZKAC3);
    }
}
