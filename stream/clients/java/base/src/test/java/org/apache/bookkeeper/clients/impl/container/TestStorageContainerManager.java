/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients.impl.container;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.junit.Test;

/**
 * Unit test for {@link StorageContainerManager}.
 */
public class TestStorageContainerManager {

    private final StorageContainerManager manager = new StorageContainerManager();
    private final Endpoint endpoint1 = Endpoint.newBuilder()
        .setHostname("128.0.0.1")
        .setPort(3181)
        .build();
    private final Endpoint endpoint2 = Endpoint.newBuilder()
        .setHostname("128.0.0.1")
        .setPort(3181)
        .build();

    @Test
    public void testGetNullStorageContainer() {
        assertNull(manager.getStorageContainer(1234L));
    }

    @Test
    public void testAddStorageContainer() {
        long groupId = 1234L;
        long revision = 5678L;
        StorageContainerInfo sc1 = StorageContainerInfo.of(
            groupId,
            revision,
            endpoint1);
        assertNull(manager.getStorageContainer(groupId));
        assertTrue(manager.replaceStorageContainer(groupId, sc1));
        assertTrue(sc1 == manager.getStorageContainer(groupId));
    }

    @Test
    public void testReplaceStorageContainerWithNewerRevision() {
        long groupId = 1234L;
        long revision1 = 5678L;
        long revision2 = 5679L;
        StorageContainerInfo sc1 = StorageContainerInfo.of(
            groupId,
            revision1,
            endpoint1);
        StorageContainerInfo sc2 = StorageContainerInfo.of(
            groupId,
            revision2,
            endpoint2);
        assertNull(manager.getStorageContainer(groupId));
        assertTrue(manager.replaceStorageContainer(groupId, sc1));
        assertTrue(sc1 == manager.getStorageContainer(groupId));
        assertTrue(manager.replaceStorageContainer(groupId, sc2));
        assertTrue(sc2 == manager.getStorageContainer(groupId));
    }

    @Test
    public void testReplaceStorageContainerWithOlderRevision() {
        long groupId = 1234L;
        long revision1 = 5678L;
        long revision2 = 5679L;
        StorageContainerInfo sc1 = StorageContainerInfo.of(
            groupId,
            revision1,
            endpoint1);
        StorageContainerInfo sc2 = StorageContainerInfo.of(
            groupId,
            revision2,
            endpoint2);
        assertNull(manager.getStorageContainer(groupId));
        assertTrue(manager.replaceStorageContainer(groupId, sc2));
        assertTrue(sc2 == manager.getStorageContainer(groupId));
        assertFalse(manager.replaceStorageContainer(groupId, sc1));
        assertTrue(sc2 == manager.getStorageContainer(groupId));
    }

    @Test
    public void testRemoveStorageContainer() {
        long groupId = 1234L;
        long revision = 5678L;
        StorageContainerInfo sc1 = StorageContainerInfo.of(
            groupId,
            revision,
            endpoint1);
        assertNull(manager.getStorageContainer(groupId));
        assertTrue(manager.replaceStorageContainer(groupId, sc1));
        assertTrue(sc1 == manager.getStorageContainer(groupId));
        manager.removeStorageContainer(groupId);
        assertNull(manager.getStorageContainer(groupId));
    }

}
