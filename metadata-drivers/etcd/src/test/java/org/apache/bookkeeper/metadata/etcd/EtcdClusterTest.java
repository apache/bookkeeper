/*
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

package org.apache.bookkeeper.metadata.etcd;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getBookiesPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getBucketsPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getClusterInstanceIdPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getLayoutKey;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getLedgersPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getReadonlyBookiesPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getScopeEndKey;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getUnderreplicationPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.getWritableBookiesPath;
import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.msResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException.MetadataStoreException;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerLayout;
import org.apache.bookkeeper.metadata.etcd.testing.EtcdTestBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cluster related operation on Etcd based registration manager.
 */
@Slf4j
public class EtcdClusterTest extends EtcdTestBase {

    private String scope;
    private RegistrationManager regMgr;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.scope = RandomStringUtils.randomAlphabetic(32);
        this.regMgr = new EtcdRegistrationManager(
            newEtcdClient(), scope
        );
    }

    @After
    @Override
    public void tearDown() throws Exception {
        this.regMgr.close();
        super.tearDown();
    }

    @Test
    public void testGetClusterInstanceIdIfClusterNotInitialized() throws Exception {
        try {
            regMgr.getClusterInstanceId();
            fail("Should fail getting cluster instance id if cluster not initialized");
        } catch (MetadataStoreException e) {
            assertTrue(e.getMessage().contains("BookKeeper is not initialized"));
        }
    }

    @Test
    public void testGetClusterInstanceId() throws Exception {
        assertClusterNotExists(etcdClient, scope);
        regMgr.initNewCluster();
        String instanceId = regMgr.getClusterInstanceId();
        UUID uuid = UUID.fromString(instanceId);
        log.info("Cluster instance id : {}", uuid);
    }

    @Test
    public void testNukeNonExistingCluster() throws Exception {
        assertClusterNotExists(etcdClient, scope);
        assertTrue(regMgr.nukeExistingCluster());
        assertClusterNotExists(etcdClient, scope);
    }

    @Test
    public void testNukeExistingCluster() throws Exception {
        assertTrue(regMgr.initNewCluster());
        assertClusterExists(etcdClient, scope);
        assertTrue(regMgr.nukeExistingCluster());
        assertClusterNotExists(etcdClient, scope);
    }

    @Test
    public void testInitNewClusterTwice() throws Exception {
        assertTrue(regMgr.initNewCluster());
        assertClusterExists(etcdClient, scope);
        String instanceId = regMgr.getClusterInstanceId();
        assertFalse(regMgr.initNewCluster());
        assertClusterExists(etcdClient, scope);
        assertEquals(instanceId, regMgr.getClusterInstanceId());
    }

    @Test
    public void testPrepareFormatNonExistingCluster() throws Exception {
        assertFalse(regMgr.prepareFormat());
    }

    @Test
    public void testPrepareFormatExistingCluster() throws Exception {
        assertTrue(regMgr.initNewCluster());
        assertClusterExists(etcdClient, scope);
        assertTrue(regMgr.prepareFormat());
    }

    @Test
    public void testNukeExistingClusterWithWritableBookies() throws Exception {
        testNukeExistingClusterWithBookies(false);
    }

    @Test
    public void testNukeExistingClusterWithReadonlyBookies() throws Exception {
        testNukeExistingClusterWithBookies(true);
    }

    private void testNukeExistingClusterWithBookies(boolean readonly) throws Exception {
        assertTrue(regMgr.initNewCluster());
        assertClusterExists(etcdClient, scope);
        createNumBookies(etcdClient, scope, 3, readonly);
        assertFalse(regMgr.nukeExistingCluster());
        assertClusterExists(etcdClient, scope);
        removeNumBookies(etcdClient, scope, 3, readonly);
        assertTrue(regMgr.nukeExistingCluster());
        assertClusterNotExists(etcdClient, scope);
    }

    @Test
    public void testNukeExistingClusterWithAllBookies() throws Exception {
        assertTrue(regMgr.initNewCluster());
        assertClusterExists(etcdClient, scope);
        createNumBookies(etcdClient, scope, 1, false);
        createNumBookies(etcdClient, scope, 2, true);
        assertFalse(regMgr.nukeExistingCluster());
        assertClusterExists(etcdClient, scope);
        removeNumBookies(etcdClient, scope, 1, false);
        removeNumBookies(etcdClient, scope, 2, true);
        assertTrue(regMgr.nukeExistingCluster());
        assertClusterNotExists(etcdClient, scope);
    }

    @Test
    public void testFormatNonExistingCluster() throws Exception {
        assertClusterNotExists(etcdClient, scope);
        assertTrue(regMgr.format());
        assertClusterExists(etcdClient, scope);
    }

    @Test
    public void testFormatExistingCluster() throws Exception {
        assertClusterNotExists(etcdClient, scope);
        assertTrue(regMgr.initNewCluster());
        assertClusterExists(etcdClient, scope);
        String clusterInstanceId = regMgr.getClusterInstanceId();
        assertTrue(regMgr.format());
        assertClusterExists(etcdClient, scope);
        assertNotEquals(clusterInstanceId, regMgr.getClusterInstanceId());
    }

    @Test
    public void testFormatExistingClusterWithBookies() throws Exception {
        assertClusterNotExists(etcdClient, scope);
        assertTrue(regMgr.initNewCluster());
        assertClusterExists(etcdClient, scope);
        String clusterInstanceId = regMgr.getClusterInstanceId();
        createNumBookies(etcdClient, scope, 3, false);
        assertFalse(regMgr.format());
        assertClusterExists(etcdClient, scope);
        assertEquals(clusterInstanceId, regMgr.getClusterInstanceId());
    }

    private static void createNumBookies(Client client,
                                         String scope,
                                         int numBookies,
                                         boolean readonly) throws Exception {
        for (int i = 0; i < numBookies; i++) {
            BookieId bookieId = BookieId.parse("bookie-" + i + ":3181");
            String bookiePath;
            if (readonly) {
                bookiePath = EtcdUtils.getReadonlyBookiePath(scope, bookieId);
            } else {
                bookiePath = EtcdUtils.getWritableBookiePath(scope, bookieId);
            }
            msResult(client.getKVClient().put(
                ByteSequence.from(bookiePath, UTF_8),
                EtcdConstants.EMPTY_BS
            ));
        }
    }

    private static void removeNumBookies(Client client,
                                         String scope,
                                         int numBookies,
                                         boolean readonly) throws Exception {
        for (int i = 0; i < numBookies; i++) {
            BookieId bookieId = BookieId.parse("bookie-" + i + ":3181");
            String bookiePath;
            if (readonly) {
                bookiePath = EtcdUtils.getReadonlyBookiePath(scope, bookieId);
            } else {
                bookiePath = EtcdUtils.getWritableBookiePath(scope, bookieId);
            }
            msResult(client.getKVClient().delete(
                ByteSequence.from(bookiePath, UTF_8)
            ));
        }
    }

    private static void assertClusterScope(Client client,
                                           String scope) throws Exception {
        GetResponse resp = msResult(
            client.getKVClient().get(
                ByteSequence.from(scope, UTF_8)));
        assertEquals(1, resp.getCount());
    }

    private static void assertClusterLayout(Client client,
                                            String scope) throws Exception {
        String layoutPath = getLayoutKey(scope);
        GetResponse resp = msResult(
            client.getKVClient().get(
                ByteSequence.from(layoutPath, UTF_8)));
        assertEquals(1, resp.getCount());
        LedgerLayout layout = LedgerLayout.parseLayout(
            resp.getKvs().get(0).getValue().getBytes()
        );
        assertEquals(
            EtcdLedgerManagerFactory.class.getName(),
            layout.getManagerFactoryClass()
        );
        assertEquals(EtcdLedgerManagerFactory.VERSION, layout.getManagerVersion());
        assertEquals(LedgerLayout.LAYOUT_FORMAT_VERSION, layout.getLayoutFormatVersion());
    }

    private static void assertClusterInstanceId(Client client,
                                                String scope) throws Exception {
        String instanceIdPath = getClusterInstanceIdPath(scope);
        GetResponse resp = msResult(
            client.getKVClient().get(ByteSequence.from(instanceIdPath, UTF_8)));
        assertEquals(1, resp.getCount());
        String instanceId = new String(resp.getKvs().get(0).getValue().getBytes(), UTF_8);
        UUID uuid = UUID.fromString(instanceId);
        log.info("Cluster instance id : {}", uuid);
    }

    private static void assertBookiesPath(Client client,
                                          String scope) throws Exception {
        String bookiesPath = getBookiesPath(scope);
        GetResponse resp = msResult(
            client.getKVClient().get(ByteSequence.from(bookiesPath, UTF_8)));
        assertEquals(1, resp.getCount());
    }

    private static void assertWritableBookiesPath(Client client,
                                                  String scope) throws Exception {
        String bookiesPath = getWritableBookiesPath(scope);
        GetResponse resp = msResult(
            client.getKVClient().get(ByteSequence.from(bookiesPath, UTF_8)));
        assertEquals(1, resp.getCount());
    }

    private static void assertReadonlyBookiesPath(Client client,
                                                  String scope) throws Exception {
        String bookiesPath = getReadonlyBookiesPath(scope);
        GetResponse resp = msResult(
            client.getKVClient().get(ByteSequence.from(bookiesPath, UTF_8)));
        assertEquals(1, resp.getCount());
    }

    private static void assertLedgersPath(Client client, String scope) throws Exception {
        String ledgersPath = getLedgersPath(scope);
        GetResponse resp = msResult(
            client.getKVClient().get(ByteSequence.from(ledgersPath, UTF_8)));
        assertEquals(1, resp.getCount());
    }

    private static void assertBucketsPath(Client client, String scope) throws Exception {
        String bucketsPath = getBucketsPath(scope);
        GetResponse resp = msResult(
            client.getKVClient().get(ByteSequence.from(bucketsPath, UTF_8)));
        assertEquals(1, resp.getCount());
    }

    private static void assertUnderreplicationPath(Client client, String scope) throws Exception {
        String urPath = getUnderreplicationPath(scope);
        GetResponse resp = msResult(
            client.getKVClient().get(ByteSequence.from(urPath, UTF_8)));
        assertEquals(1, resp.getCount());
    }

    private static void assertClusterExists(Client client, String scope) throws Exception {
        assertClusterScope(client, scope);
        assertClusterLayout(client, scope);
        assertClusterInstanceId(client, scope);
        assertBookiesPath(client, scope);
        assertWritableBookiesPath(client, scope);
        assertReadonlyBookiesPath(client, scope);
        assertLedgersPath(client, scope);
        assertBucketsPath(client, scope);
        assertUnderreplicationPath(client, scope);
    }

    private static void assertClusterNotExists(Client client, String scope) throws Exception {
        GetResponse response = msResult(
            client.getKVClient().get(
                ByteSequence.from(scope, UTF_8),
                GetOption.newBuilder()
                    .withRange(ByteSequence.from(getScopeEndKey(scope), UTF_8))
                    .build()));
        assertEquals(0, response.getCount());
    }

}
