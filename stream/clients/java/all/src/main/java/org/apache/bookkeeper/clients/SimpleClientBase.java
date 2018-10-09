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

package org.apache.bookkeeper.clients;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.impl.container.StorageContainerClientInterceptor;
import org.apache.bookkeeper.clients.utils.ClientResources;
import org.apache.bookkeeper.clients.utils.GrpcChannels;
import org.apache.bookkeeper.clients.utils.RetryUtils;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;

/**
 * Simple client implementation base.
 */
public class SimpleClientBase extends AbstractAutoAsyncCloseable {

    protected final StorageClientSettings settings;
    protected final Resource<OrderedScheduler> schedulerResource;
    protected final OrderedScheduler scheduler;
    protected final ManagedChannel managedChannel;
    protected final boolean ownChannel;
    protected final Channel channel;
    protected final RetryUtils retryUtils;

    protected SimpleClientBase(StorageClientSettings settings) {
        this(settings, ClientResources.create().scheduler());
    }

    protected SimpleClientBase(StorageClientSettings settings,
                               Resource<OrderedScheduler> schedulerResource) {
        this(
            settings,
            schedulerResource,
            GrpcChannels.createChannelBuilder(settings.serviceUri(), settings).build(),
            true);
    }

    protected SimpleClientBase(StorageClientSettings settings,
                               Resource<OrderedScheduler> schedulerResource,
                               ManagedChannel managedChannel,
                               boolean ownChannel) {
        this.settings = settings;
        this.managedChannel = managedChannel;
        this.ownChannel = ownChannel;
        this.channel = ClientInterceptors.intercept(
            managedChannel,
            new StorageContainerClientInterceptor(0L));
        this.schedulerResource = schedulerResource;
        this.scheduler = SharedResourceManager.shared().get(schedulerResource);
        this.retryUtils = RetryUtils.create(settings.backoffPolicy(), scheduler);
    }

    @Override
    protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
        if (ownChannel) {
            managedChannel.shutdown();
        }
        SharedResourceManager.shared().release(schedulerResource, scheduler);
        closeFuture.complete(null);
    }
}
