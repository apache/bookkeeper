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

package org.apache.bookkeeper.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Test;

/**
 * Test store/read/delete ledger layout operations on zookeeper.
 */
public class ZkLedgerLayoutTest extends BookKeeperClusterTestCase {

    public ZkLedgerLayoutTest() {
        super(0);
    }

    @Test
    public void testLedgerLayout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setLedgerManagerFactoryClass(HierarchicalLedgerManagerFactory.class);
        String ledgerRootPath = "/testLedgerLayout";
        ZkLayoutManager zkLayoutManager = new ZkLayoutManager(zkc, ledgerRootPath, Ids.OPEN_ACL_UNSAFE);

        zkc.create(ledgerRootPath, new byte[0],
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        assertEquals(null, zkLayoutManager.readLedgerLayout());

        String testName = "foobar";
        int testVersion = 0xdeadbeef;
        // use layout defined in configuration also create it in zookeeper
        LedgerLayout layout2 = new LedgerLayout(testName, testVersion);
        zkLayoutManager.storeLedgerLayout(layout2);

        LedgerLayout layout = zkLayoutManager.readLedgerLayout();
        assertEquals(testName, layout.getManagerFactoryClass());
        assertEquals(testVersion, layout.getManagerVersion());
    }

    private void writeLedgerLayout(
                                  String ledgersRootPath,
                                  String managerType,
                                  int managerVersion, int layoutVersion)
        throws Exception {
        LedgerLayout layout = new LedgerLayout(managerType, managerVersion);

        Field f = LedgerLayout.class.getDeclaredField("layoutFormatVersion");
        f.setAccessible(true);
        f.set(layout, layoutVersion);
        ZkLayoutManager zkLayoutManager = new ZkLayoutManager(zkc, ledgersRootPath, Ids.OPEN_ACL_UNSAFE);

        zkLayoutManager.storeLedgerLayout(layout);
    }

    @Test
    public void testBadVersionLedgerLayout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        // write bad version ledger layout
        writeLedgerLayout(conf.getZkLedgersRootPath(),
                          FlatLedgerManagerFactory.class.getName(),
                          FlatLedgerManagerFactory.CUR_VERSION,
                          LedgerLayout.LAYOUT_FORMAT_VERSION + 1);

        ZkLayoutManager zkLayoutManager = new ZkLayoutManager(zkc, conf.getZkLedgersRootPath(), Ids.OPEN_ACL_UNSAFE);

        try {
            zkLayoutManager.readLedgerLayout();
            fail("Shouldn't reach here!");
        } catch (IOException ie) {
            assertTrue("Invalid exception", ie.getMessage().contains("version not compatible"));
        }
    }

    @Test
    public void testAbsentLedgerManagerLayout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        String ledgersLayout = conf.getZkLedgersRootPath() + "/"
                + BookKeeperConstants.LAYOUT_ZNODE;
        // write bad format ledger layout
        StringBuilder sb = new StringBuilder();
        sb.append(LedgerLayout.LAYOUT_FORMAT_VERSION).append("\n");
        zkc.create(ledgersLayout, sb.toString().getBytes(),
                                 Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        ZkLayoutManager zkLayoutManager = new ZkLayoutManager(zkc, conf.getZkLedgersRootPath(), Ids.OPEN_ACL_UNSAFE);

        try {
            zkLayoutManager.readLedgerLayout();
            fail("Shouldn't reach here!");
        } catch (IOException ie) {
            assertTrue("Invalid exception", ie.getMessage().contains("version absent from"));
        }
    }

    @Test
    public void testBaseLedgerManagerLayout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        String rootPath = conf.getZkLedgersRootPath();
        String ledgersLayout = rootPath + "/"
                + BookKeeperConstants.LAYOUT_ZNODE;
        // write bad format ledger layout
        StringBuilder sb = new StringBuilder();
        sb.append(LedgerLayout.LAYOUT_FORMAT_VERSION).append("\n")
          .append(FlatLedgerManagerFactory.class.getName());
        zkc.create(ledgersLayout, sb.toString().getBytes(),
                                 Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        ZkLayoutManager zkLayoutManager = new ZkLayoutManager(zkc, rootPath, Ids.OPEN_ACL_UNSAFE);

        try {
            zkLayoutManager.readLedgerLayout();
            fail("Shouldn't reach here!");
        } catch (IOException ie) {
            assertTrue("Invalid exception", ie.getMessage().contains("Invalid Ledger Manager"));
        }
    }

    @Test
    public void testReadV1LedgerManagerLayout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        // write v1 ledger layout
        writeLedgerLayout(conf.getZkLedgersRootPath(),
                          FlatLedgerManagerFactory.NAME,
                          FlatLedgerManagerFactory.CUR_VERSION, 1);
        ZkLayoutManager zkLayoutManager = new ZkLayoutManager(zkc, conf.getZkLedgersRootPath(), Ids.OPEN_ACL_UNSAFE);

        LedgerLayout layout = zkLayoutManager.readLedgerLayout();

        assertNotNull("Should not be null", layout);
        assertEquals(FlatLedgerManagerFactory.NAME, layout.getManagerFactoryClass());
        assertEquals(FlatLedgerManagerFactory.CUR_VERSION, layout.getManagerVersion());
        assertEquals(1, layout.getLayoutFormatVersion());
    }
}
