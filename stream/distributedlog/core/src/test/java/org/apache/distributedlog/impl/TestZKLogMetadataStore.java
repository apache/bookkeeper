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
package org.apache.distributedlog.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.net.URI;
import java.util.Optional;
import java.util.Set;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.distributedlog.TestZooKeeperClientBuilder;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test ZK based metadata store.
 */
public class TestZKLogMetadataStore extends TestDistributedLogBase {

    private static final  int zkSessionTimeoutMs = 2000;

    @Rule
    public TestName runtime = new TestName();
    protected final DistributedLogConfiguration baseConf =
            new DistributedLogConfiguration();
    protected ZooKeeperClient zkc;
    protected ZKLogMetadataStore metadataStore;
    protected OrderedScheduler scheduler;
    protected URI uri;

    @Before
    public void setup() throws Exception {
        zkc = TestZooKeeperClientBuilder.newBuilder()
                .uri(createDLMURI("/"))
                .sessionTimeoutMs(zkSessionTimeoutMs)
                .build();
        scheduler = OrderedScheduler.newSchedulerBuilder()
                .name("test-zk-logmetadata-store")
                .numThreads(1)
                .build();
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.addConfiguration(baseConf);
        this.uri = createDLMURI("/" + runtime.getMethodName());
        metadataStore = new ZKLogMetadataStore(conf, uri, zkc, scheduler);
    }

    @After
    public void teardown() throws Exception {
        if (null != zkc) {
            zkc.close();
        }
        if (null != scheduler) {
            scheduler.shutdown();
        }
    }

    private void createLogInNamespace(URI uri, String logName) throws Exception {
        String logPath = uri.getPath() + "/" + logName;
        Utils.zkCreateFullPathOptimistic(zkc, logPath, new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test(timeout = 60000)
    public void testCreateLog() throws Exception {
        assertEquals(uri, Utils.ioResult(metadataStore.createLog("test")));
    }

    @Test(timeout = 60000)
    public void testGetLogLocation() throws Exception {
        Optional<URI> uriOptional = Utils.ioResult(metadataStore.getLogLocation("test"));
        assertTrue(uriOptional.isPresent());
        assertEquals(uri, uriOptional.get());
    }

    @Test(timeout = 60000)
    public void testGetLogs() throws Exception {
        Set<String> logs = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            String logName = "test-" + i;
            logs.add(logName);
            createLogInNamespace(uri, logName);
        }
        Set<String> result = Sets.newHashSet(Utils.ioResult(metadataStore.getLogs("")));
        assertEquals(10, result.size());
        assertTrue(Sets.difference(logs, result).isEmpty());
    }

    @Test(timeout = 60000)
    public void testGetLogsPrefix() throws Exception {
        Set<String> logs = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            String logName = "test-" + i;
            logs.add(logName);
            createLogInNamespace(uri, "test/" + logName);
        }
        Set<String> result = Sets.newHashSet(Utils.ioResult(metadataStore.getLogs("test")));
        assertEquals(10, result.size());
        assertTrue(Sets.difference(logs, result).isEmpty());
    }
}
