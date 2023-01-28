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
package org.apache.distributedlog.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.TestZooKeeperClientBuilder;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.ZooKeeperClusterTestCase;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test Utils.
 */
public class TestUtils extends ZooKeeperClusterTestCase {

    private static final  int sessionTimeoutMs = 30000;

    private ZooKeeperClient zkc;

    @Before
    public void setup() throws Exception {
        zkc = TestZooKeeperClientBuilder.newBuilder()
                .name("zkc")
                .uri(DLMTestUtil.createDLMURI(zkPort, "/"))
                .sessionTimeoutMs(sessionTimeoutMs)
                .build();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    @Test(timeout = 60000)
    public void testZkAsyncCreateFulPathOptimisticRecursive() throws Exception {
        String path1 = "/a/b/c/d";
        Optional<String> parentPathShouldNotCreate = Optional.empty();
        final CountDownLatch doneLatch1 = new CountDownLatch(1);
        Utils.zkAsyncCreateFullPathOptimisticRecursive(zkc, path1, parentPathShouldNotCreate,
                new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        doneLatch1.countDown();
                    }
                }, null);
        doneLatch1.await();
        assertNotNull(zkc.get().exists(path1, false));

        String path2 = "/a/b/c/d/e/f/g";
        parentPathShouldNotCreate = Optional.of("/a/b/c/d/e");
        final CountDownLatch doneLatch2 = new CountDownLatch(1);
        Utils.zkAsyncCreateFullPathOptimisticRecursive(zkc, path2, parentPathShouldNotCreate,
                new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        doneLatch2.countDown();
                    }
                }, null);
        doneLatch2.await();
        assertNull(zkc.get().exists("/a/b/c/d/e", false));
        assertNull(zkc.get().exists("/a/b/c/d/e/f", false));
        assertNull(zkc.get().exists("/a/b/c/d/e/f/g", false));

        parentPathShouldNotCreate = Optional.of("/a/b");
        final CountDownLatch doneLatch3 = new CountDownLatch(1);
        Utils.zkAsyncCreateFullPathOptimisticRecursive(zkc, path2, parentPathShouldNotCreate,
                new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        doneLatch3.countDown();
                    }
                }, null);
        doneLatch3.await();
        assertNotNull(zkc.get().exists(path2, false));
    }

    @Test(timeout = 60000)
    public void testZkGetData() throws Exception {
        String path1 = "/zk-get-data/non-existent-path";
        Versioned<byte[]> data = Utils.ioResult(Utils.zkGetData(zkc.get(), path1, false));
        assertNull("No data should return from non-existent-path", data.getValue());
        assertNull("No version should return from non-existent-path", data.getVersion());

        String path2 = "/zk-get-data/path2";
        byte[] rawData = "test-data".getBytes(UTF_8);
        Utils.ioResult(Utils.zkAsyncCreateFullPathOptimistic(zkc, path2, rawData,
                zkc.getDefaultACL(), CreateMode.PERSISTENT));
        data = Utils.ioResult(Utils.zkGetData(zkc.get(), path2, false));
        assertArrayEquals("Data should return as written",
                rawData, data.getValue());
        assertEquals("Version should be zero",
                0L, ((LongVersion) data.getVersion()).getLongVersion());
    }

    @Test(timeout = 60000)
    public void testGetParent() throws Exception {
        String path1 = null;
        assertNull("parent of a null path is null", Utils.getParent(path1));

        String path2 = "";
        assertNull("parent of an empty string is null", Utils.getParent(path2));

        String path3 = "abcdef";
        assertNull("parent of a string with no / is null", Utils.getParent(path3));

        String path4 = "/test/test2";
        assertEquals("parent of a /test/test2 is /test", "/test", Utils.getParent(path4));

        String path5 = "/test/test2/";
        assertEquals("parent of a " + path5 + " is /test", "/test", Utils.getParent(path5));

        String path6 = "/test";
        assertEquals("parent of " + path6 + " is /", "/", Utils.getParent(path6));

        String path7 = "//";
        assertEquals("parent of " + path7 + " is /", "/", Utils.getParent(path7));

        String path8 = "/";
        assertNull("parent of " + path8 + " is null", Utils.getParent(path8));
    }

}
