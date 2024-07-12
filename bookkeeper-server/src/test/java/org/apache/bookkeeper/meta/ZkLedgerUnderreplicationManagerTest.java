/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.meta;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.DataFormats.UnderreplicatedLedgerFormat;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.suites.BookKeeperClusterTestSuite;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test {@link ZkLedgerUnderreplicationManager}.
 */
public class ZkLedgerUnderreplicationManagerTest extends BookKeeperClusterTestSuite {

    @BeforeClass
    public static void setUpCluster() throws Exception {
        BookKeeperClusterTestSuite.setUpCluster(0);
    }

    @AfterClass
    public static void tearDownCluster() throws Exception {
        BookKeeperClusterTestSuite.tearDownCluster();
    }

    private static String getZooKeeperConnectString() throws Exception {
        String[] serviceHosts = metadataStore.getServiceUri().getServiceHosts();
        return StringUtils.join(serviceHosts, ',');
    }

    private static ZooKeeper createZooKeeper() throws Exception {
        return ZooKeeperClient.newBuilder()
            .connectString(getZooKeeperConnectString())
            .connectRetryPolicy(
                new BoundExponentialBackoffRetryPolicy(1, 10, 100))
            .operationRetryPolicy(
                new BoundExponentialBackoffRetryPolicy(1, 10, 100))
            .sessionTimeoutMs(60000)
            .build();
    }

    private ZooKeeper zk;
    private ZkLedgerUnderreplicationManager urMgr;

    @Before
    public void setUp() throws Exception {
        this.zk = createZooKeeper();
        ServerConfiguration conf = new ServerConfiguration(baseServerConf);
        conf.setStoreSystemTimeAsLedgerUnderreplicatedMarkTime(true);
        this.urMgr = new ZkLedgerUnderreplicationManager(conf, zk);
    }

    @After
    public void tearDown() throws Exception {
        if (null != urMgr) {
            this.urMgr.close();
        }
        if (null != zk) {
            this.zk.close();
        }
    }

    /**
     * Test basic operation on {@link ZkLedgerUnderreplicationManager#markLedgerUnderreplicatedAsync(long, Collection)}.
     */
    @Test
    public void testMarkLedgerUnderreplicatedBasic() throws Exception {
        long ledgerId = 0xabcdef;
        Collection<String> missingBookies = Lists.newArrayList("bookie-1");

        long prevCtime = -1L;

        // mark when it hasn't been marked before
        FutureUtils.result(urMgr.markLedgerUnderreplicatedAsync(ledgerId, missingBookies));
        UnderreplicatedLedger urLedgerFormat = urMgr.getLedgerUnreplicationInfo(ledgerId);
        assertEquals(missingBookies, urLedgerFormat.getReplicaList());
        assertTrue(urLedgerFormat.getCtime() > prevCtime);
        prevCtime = urLedgerFormat.getCtime();

        // mark when it has been marked. but the missing bookies already duplicated there
        FutureUtils.result(urMgr.markLedgerUnderreplicatedAsync(ledgerId, missingBookies));
        urLedgerFormat = urMgr.getLedgerUnreplicationInfo(ledgerId);
        assertEquals(missingBookies, urLedgerFormat.getReplicaList());
        assertTrue(urLedgerFormat.getCtime() >= prevCtime);
        prevCtime = urLedgerFormat.getCtime();

        // mark with new bookies when it has been marked
        Collection<String> newMissingBookies = Lists.newArrayList("bookie-2", "bookie-3");
        FutureUtils.result(urMgr.markLedgerUnderreplicatedAsync(ledgerId, newMissingBookies));
        urLedgerFormat = urMgr.getLedgerUnreplicationInfo(ledgerId);
        assertEquals(
            Lists.newArrayList("bookie-1", "bookie-2", "bookie-3"),
            urLedgerFormat.getReplicaList()
        );
        assertTrue(urLedgerFormat.getCtime() >= prevCtime);
    }

    /**
     * Test {@link ZkLedgerUnderreplicationManager#markLedgerUnderreplicatedAsync(long, Collection)} handles corrupted
     * data.
     */
    @Test
    public void testMarkLedgerWithCorruptedDataExists() throws Exception {
        long ledgerId = 0xabcdee;
        String ledgerPath = urMgr.getUrLedgerZnode(ledgerId);
        ZkUtils.createFullPathOptimistic(
            zk, ledgerPath, "junk data".getBytes(UTF_8), ZkUtils.getACLs(baseServerConf), CreateMode.PERSISTENT);
        Collection<String> missingBookies = Lists.newArrayList("bookie-1");
        try {
            FutureUtils.result(urMgr.markLedgerUnderreplicatedAsync(ledgerId, missingBookies));
            fail("Should fail to mark ledger underreplicated if there is already corrupted data on zookeeper");
        } catch (ReplicationException re) {
            assertTrue(re.getMessage().contains("Invalid underreplicated ledger data for ledger " + ledgerPath));
        }
        byte[] data = zk.getData(ledgerPath, null, null);
        assertEquals("junk data", new String(data, UTF_8));
    }

    @Test
    public void testMarkLedgerUnderreplicatedConcurrently() throws Exception {
        final int numLedgers = 20;
        List<CompletableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(numLedgers);
        long ledgerId = 0xabcc00;
        Set<String> expectedBookies = Sets.newHashSet();
        for (int i = 0; i < numLedgers; i++) {
            futures.add(
                urMgr.markLedgerUnderreplicatedAsync(
                    ledgerId, Lists.newArrayList("bookie-" + i)));
            expectedBookies.add("bookie-" + i);
        }
        FutureUtils.result(FutureUtils.collect(futures));

        UnderreplicatedLedger urLedgerFormat = urMgr.getLedgerUnreplicationInfo(ledgerId);
        Set<String> actualBookies = Sets.newHashSet();
        actualBookies.addAll(urLedgerFormat.getReplicaList());

        assertEquals(expectedBookies, actualBookies);
    }

    @Test
    public void testMarkLedgerUnderreplicatedConcurrentlyWithDifferentClients() throws Exception {
        final int numLedgers = 20;
        List<ZooKeeper> zks = new ArrayList<>(numLedgers);
        List<ZkLedgerUnderreplicationManager> urMgrs = new ArrayList<>(numLedgers);

        for (int i = 0; i < numLedgers; i++) {
            zks.add(createZooKeeper());
            urMgrs.add(new ZkLedgerUnderreplicationManager(baseServerConf, zks.get(i)));
        }

        List<CompletableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(numLedgers);
        long ledgerId = 0xabcd00;
        Set<String> expectedBookies = Sets.newHashSet();
        for (int i = 0; i < numLedgers; i++) {
            futures.add(
                urMgrs.get(i).markLedgerUnderreplicatedAsync(
                    ledgerId, Lists.newArrayList("bookie-" + i)));
            expectedBookies.add("bookie-" + i);
        }

        FutureUtils.result(FutureUtils.collect(futures));

        UnderreplicatedLedger urLedgerFormat = urMgr.getLedgerUnreplicationInfo(ledgerId);
        Set<String> actualBookies = Sets.newHashSet();
        actualBookies.addAll(urLedgerFormat.getReplicaList());

        assertEquals(expectedBookies, actualBookies);

        for (LedgerUnderreplicationManager urMgr : urMgrs) {
            urMgr.close();
        }
        for (ZooKeeper zk : zks) {
            zk.close();
        }
    }


    @Test
    public void testMarkLedgerUnderreplicatedWhenSessionExpired() throws Exception {
        final long ledgerId = 0xabbd00;
        @Cleanup
        ZooKeeper zk = new ZooKeeper(getZooKeeperConnectString(), 60000, null);
        @Cleanup
        LedgerUnderreplicationManager urMgr = new ZkLedgerUnderreplicationManager(baseServerConf, zk);
        // open another zookeeper client to expire current session
        @Cleanup
        ZooKeeper newZk = new ZooKeeper(
            getZooKeeperConnectString(), 60000, null, zk.getSessionId(), zk.getSessionPasswd());
        try {
            FutureUtils.result(urMgr.markLedgerUnderreplicatedAsync(
                ledgerId, Lists.newArrayList("bookie-1")));
            fail("Should fail if encountered zookeeper exceptions");
        } catch (KeeperException ke) {
            // expected
        }
        try {
            UnderreplicatedLedger urLedgerFormat =
                ZkLedgerUnderreplicationManagerTest.this.urMgr.getLedgerUnreplicationInfo(ledgerId);
            fail("The ledger shouldn't been marked as underreplicated");
        } catch (ReplicationException.UnavailableException ue) {
            // expected
        }
    }


}
