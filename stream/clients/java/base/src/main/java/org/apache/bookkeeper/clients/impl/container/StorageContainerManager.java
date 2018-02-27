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

import com.google.common.collect.Maps;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;

/**
 * A manager manages the mapping between storage containers and range servers.
 */
@Slf4j
public class StorageContainerManager implements AutoCloseable {

    @GuardedBy("storageContainers")
    private final Map<Long, StorageContainerInfo> storageContainers;

    public StorageContainerManager() {
        this.storageContainers = Maps.newHashMap();
    }

    @Nullable
    public StorageContainerInfo getStorageContainer(long groupId) {
        synchronized (storageContainers) {
            return this.storageContainers.get(groupId);
        }
    }

    public boolean replaceStorageContainer(long groupId, StorageContainerInfo groupInfo) {
        synchronized (storageContainers) {
            StorageContainerInfo oldGroupInfo = storageContainers.get(groupId);
            if (null == oldGroupInfo || oldGroupInfo.getRevision() < groupInfo.getRevision()) {
                log.info("Updated the storage container info for group {} : ", groupId, groupInfo);
                storageContainers.put(groupId, groupInfo);
                return true;
            }
            return false;
        }
    }

    public void removeStorageContainer(long groupId) {
        synchronized (storageContainers) {
            storageContainers.remove(groupId);
        }
    }

    @Override
    public void close() {
        synchronized (storageContainers) {
            storageContainers.clear();
        }
    }
}
