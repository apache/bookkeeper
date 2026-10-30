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
package org.apache.distributedlog.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.net.URI;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.LogWriter;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test selecting the zookeeper servers of a namespace between the ones stored in its DL config and its uri.
 */
public class TestBKNamespaceDriverZkServers extends TestDistributedLogBase {

    private static final String INTERNAL_PATTERN = ".*\\.svc\\.cluster\\.local.*";
    private static final String INTERNAL_ZK = "zk-0.zk-headless.ns.svc.cluster.local:2181";
    private static final String URI_ZK = "zk.example.com:2181";

    @Rule
    public TestName runtime = new TestName();

    @Test
    public void testSelectWhenDisabled() {
        assertEquals(INTERNAL_ZK, BKNamespaceDriver.selectZkServers(INTERNAL_ZK, URI_ZK, ""));
        assertEquals(INTERNAL_ZK, BKNamespaceDriver.selectZkServers(INTERNAL_ZK, URI_ZK, null));
    }

    @Test
    public void testSelectIgnoresMatchingStoredServers() {
        assertEquals(URI_ZK, BKNamespaceDriver.selectZkServers(INTERNAL_ZK, URI_ZK, INTERNAL_PATTERN));
    }

    @Test
    public void testSelectIgnoresWhenAnyServerMatches() {
        String stored = URI_ZK + "," + INTERNAL_ZK + ",zk-2.zk-headless.ns.svc.cluster.local:2181";
        assertEquals(URI_ZK, BKNamespaceDriver.selectZkServers(stored, URI_ZK, INTERNAL_PATTERN));
    }

    @Test
    public void testSelectKeepsNonMatchingStoredServers() {
        String stored = "other-zk.example.com:2181";
        assertEquals(stored, BKNamespaceDriver.selectZkServers(stored, URI_ZK, INTERNAL_PATTERN));
        assertEquals(URI_ZK, BKNamespaceDriver.selectZkServers(URI_ZK, "ignored:2181", INTERNAL_PATTERN));
    }

    @Test
    public void testSelectKeepsChrootOfUriServers() {
        String uriZk = URI_ZK + "/chroot";
        assertEquals(uriZk, BKNamespaceDriver.selectZkServers(INTERNAL_ZK, uriZk, INTERNAL_PATTERN));
    }

    @Test
    public void testSelectNullStoredServers() {
        assertNull(BKNamespaceDriver.selectZkServers(null, URI_ZK, INTERNAL_PATTERN));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectInvalidPattern() {
        BKNamespaceDriver.selectZkServers(INTERNAL_ZK, URI_ZK, "*(");
    }

    @Test
    public void testConfiguration() {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        assertEquals("", conf.getZkServersIgnorePattern());
        conf.setZkServersIgnorePattern(INTERNAL_PATTERN);
        assertEquals(INTERNAL_PATTERN, conf.getZkServersIgnorePattern());
    }

    @Test(timeout = 120000)
    public void testReadWriteBaseline() throws Exception {
        // control: a valid stored config, no ignore pattern
        writeAndRead(new BKDLConfig(zkServers, "/ledgers"), "");
    }

    /**
     * The namespace config stored in zookeeper points the bookkeeper clients to an in-cluster name that cannot be
     * resolved. With the ignore pattern set, the namespace must connect with the servers of its uri instead.
     */
    @Test(timeout = 120000)
    public void testReadWriteWithInternalZkServersStored() throws Exception {
        writeAndRead(new BKDLConfig(zkServers, zkServers, INTERNAL_ZK, INTERNAL_ZK, "/ledgers"), INTERNAL_PATTERN);
    }

    private void writeAndRead(BKDLConfig bkdlConfig, String ignorePattern) throws Exception {
        URI uri = createDLMURI("/" + runtime.getMethodName());
        DLMetadata.create(bkdlConfig).create(uri);

        DistributedLogConfiguration newConf = new DistributedLogConfiguration();
        newConf.addConfiguration(conf);
        newConf.setZkServersIgnorePattern(ignorePattern);
        Namespace namespace = NamespaceBuilder.newBuilder().conf(newConf).uri(uri).build();
        try {
            DistributedLogManager dlm = namespace.openLog("test-stream");
            LogWriter writer = dlm.startLogSegmentNonPartitioned();
            writer.write(DLMTestUtil.getLogRecordInstance(1L));
            writer.close();

            LogReader reader = dlm.getInputStream(1L);
            LogRecordWithDLSN record = reader.readNext(false);
            assertNotNull("Should read back the record written", record);
            assertEquals(1L, record.getTransactionId());
            reader.close();
            dlm.close();
        } finally {
            namespace.close();
        }
    }
}
