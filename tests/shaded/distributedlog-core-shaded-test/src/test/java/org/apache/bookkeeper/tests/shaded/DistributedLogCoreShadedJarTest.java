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
package org.apache.bookkeeper.tests.shaded;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test whether the distributedlog-core-shaded jar is generated correctly.
 */
public class DistributedLogCoreShadedJarTest {

    @Test(expected = ClassNotFoundException.class)
    public void testProtobufIsShaded() throws Exception {
        Class.forName("com.google.protobuf.Message");
    }

    @Test
    public void testProtobufShadedPath() throws Exception {
        Class.forName("dlshade.com.google.protobuf.Message");
    }

    @Test(expected = ClassNotFoundException.class)
    public void testGuavaIsShaded() throws Exception {
        Class.forName("com.google.common.cache.Cache");
    }

    @Test
    public void testGuavaShadedPath() throws Exception {
        Class.forName("dlshade.com.google.common.cache.Cache");
        assertTrue(true);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testZooKeeperIsShaded() throws Exception {
        Class.forName("org.apache.zookeeper.ZooKeeper");
    }

    @Test
    public void testZooKeeperShadedPath() throws Exception {
        Class.forName("dlshade.org.apache.zookeeper.ZooKeeper");
    }

    @Test
    public void testBookKeeperCommon() throws Exception {
        Class.forName("org.apache.bookkeeper.util.OrderedSafeExecutor");
        assertTrue(true);
    }

    @Test
    public void testBookKeeperProto() throws Exception {
        Class.forName("org.apache.bookkeeper.proto.BookkeeperProtocol");
        assertTrue(true);
    }

    @Test
    public void testCirceChecksum() throws Exception {
        Class.forName("com.scurrilous.circe.checksum.Crc32cIntChecksum");
        assertTrue(true);
    }

    @Test
    public void testDistributedLogCommon() throws Exception {
        Class.forName("org.apache.distributedlog.common.concurrent.AsyncSemaphore");
        assertTrue(true);
    }

    @Test
    public void testDistributedLogProto() throws Exception {
        Class.forName("org.apache.distributedlog.DLSN");
        assertTrue(true);
    }

    @Test
    public void testDistributedLogCore() throws Exception {
        Class.forName("org.apache.distributedlog.api.AsyncLogReader");
        assertTrue(true);
    }
}
