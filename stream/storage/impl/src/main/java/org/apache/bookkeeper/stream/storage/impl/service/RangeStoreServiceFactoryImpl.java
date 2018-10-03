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

package org.apache.bookkeeper.stream.storage.impl.service;

import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.bookkeeper.stream.storage.api.metadata.RangeStoreService;
import org.apache.bookkeeper.stream.storage.api.service.RangeStoreServiceFactory;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;

/**
 * Default implementation of {@link RangeStoreServiceFactory}.
 */
public class RangeStoreServiceFactoryImpl implements RangeStoreServiceFactory {

    private final StorageConfiguration storageConf;
    private final StorageContainerPlacementPolicy rangePlacementPolicy;
    private final Resource<OrderedScheduler> schedulerResource;
    private final OrderedScheduler scheduler;
    private final MVCCStoreFactory storeFactory;
    private final StorageServerClientManager clientManager;

    public RangeStoreServiceFactoryImpl(StorageConfiguration storageConf,
                                        StorageContainerPlacementPolicy rangePlacementPolicy,
                                        Resource<OrderedScheduler> schedulerResource,
                                        MVCCStoreFactory storeFactory,
                                        StorageServerClientManager clientManager) {
        this.storageConf = storageConf;
        this.rangePlacementPolicy = rangePlacementPolicy;
        this.schedulerResource = schedulerResource;
        this.scheduler = SharedResourceManager.shared().get(schedulerResource);
        this.storeFactory = storeFactory;
        this.clientManager = clientManager;
    }

    @Override
    public RangeStoreService createService(long scId) {
        return new RangeStoreServiceImpl(
            scId,
            rangePlacementPolicy,
            scheduler,
            storeFactory,
            clientManager);
    }

    @Override
    public void close() {
        SharedResourceManager.shared().release(schedulerResource, scheduler);
    }
}
