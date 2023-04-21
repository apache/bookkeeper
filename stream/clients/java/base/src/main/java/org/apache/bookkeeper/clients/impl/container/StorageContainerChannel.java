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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.exceptions.StorageContainerException;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannelManager;
import org.apache.bookkeeper.clients.impl.internal.api.LocationClient;
import org.apache.bookkeeper.clients.utils.ClientConstants;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;
import org.apache.bookkeeper.common.util.IRevisioned;
import org.apache.bookkeeper.common.util.Revisioned;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerEndpoint;

/**
 * A client place holder for managing information of storage containers.
 */
@Slf4j
public class StorageContainerChannel {

    private final long scId;
    private final StorageServerChannelManager channelManager;
    private final LocationClient locationClient;
    private final ScheduledExecutorService executor;

    @GuardedBy("this")
    private StorageContainerInfo scInfo = null;
    @GuardedBy("this")
    private CompletableFuture<StorageServerChannel> rsChannelFuture = null;

    public StorageContainerChannel(long scId,
                                   StorageServerChannelManager channelManager,
                                   LocationClient locationClient,
                                   ScheduledExecutorService executor) {
        this.scId = scId;
        this.channelManager = channelManager;
        this.locationClient = locationClient;
        this.executor = executor;
    }

    public long getStorageContainerId() {
        return this.scId;
    }

    public synchronized StorageContainerInfo getStorageContainerInfo() {
        return scInfo;
    }

    public synchronized CompletableFuture<StorageServerChannel> getStorageServerChannelFuture() {
        return rsChannelFuture;
    }

    public synchronized void resetStorageServerChannelFuture() {
        rsChannelFuture = null;
    }

    public synchronized boolean resetStorageServerChannelFuture(CompletableFuture<StorageServerChannel> oldFuture) {
        if (oldFuture != null) {
            // we only reset the channel that we expect to reset
            if (rsChannelFuture == oldFuture) {
                rsChannelFuture = null;
                return true;
            } else {
                return false;
            }
        } else {
            rsChannelFuture = null;
            return true;
        }
    }

    @VisibleForTesting
    public synchronized void setStorageServerChannelFuture(CompletableFuture<StorageServerChannel> rsChannelFuture) {
        this.rsChannelFuture = rsChannelFuture;
    }

    public CompletableFuture<StorageServerChannel> getStorageContainerChannelFuture() {
        CompletableFuture<StorageServerChannel> channelFuture;
        synchronized (this) {
            if (null != rsChannelFuture) {
                return rsChannelFuture;
            }
            channelFuture = rsChannelFuture = FutureUtils.createFuture();
        }
        fetchStorageContainerInfo();
        return channelFuture;
    }

    /**
     * Retrieve the latest storage container info.
     */
    private void fetchStorageContainerInfo() {
        long scRevision;
        synchronized (this) {
            if (null == scInfo) {
                scRevision = IRevisioned.ANY_REVISION;
            } else {
                scRevision = scInfo.getRevision();
            }
        }
        Revisioned<Long> groupId = Revisioned.of(scId, scRevision);
        this.locationClient
            .locateStorageContainers(Lists.newArrayList(groupId))
            .whenCompleteAsync((scEndpoints, cause) -> {
                if (null != cause) {
                    handleFetchStorageContainerInfoFailure(cause);
                    return;
                }
                handleFetchStorageContainerInfoSuccess(scEndpoints);
            }, executor);
    }

    private void handleFetchStorageContainerInfoFailure(Throwable cause) {
        log.info("Failed to fetch info of storage container ({}) - '{}'. Retry in {} ms ...",
            new Object[]{scId, cause.getMessage(), ClientConstants.DEFAULT_BACKOFF_START_MS});
        executor.schedule(() -> {
            fetchStorageContainerInfo();
        }, ClientConstants.DEFAULT_BACKOFF_START_MS, TimeUnit.MILLISECONDS);
    }

    private void handleFetchStorageContainerInfoSuccess(
        List<OneStorageContainerEndpointResponse> storageContainerEndpoints) {
        if (storageContainerEndpoints.size() != 1) {
            handleFetchStorageContainerInfoFailure(new Exception(
                "Expected only one storage container endpoint. But found " + storageContainerEndpoints.size()
                    + " storage container endpoints."));
            return;
        }
        OneStorageContainerEndpointResponse response = storageContainerEndpoints.get(0);
        if (StatusCode.SUCCESS != response.getStatusCode()) {
            handleFetchStorageContainerInfoFailure(
                new StorageContainerException(response.getStatusCode(),
                    "fail to fetch location for storage container (" + scId + ")"));
            return;
        }
        StorageContainerEndpoint endpoint = response.getEndpoint();
        if (null != scInfo && scInfo.getRevision() >= endpoint.getRevision()) {
            handleFetchStorageContainerInfoFailure(
                new StorageContainerException(StatusCode.STALE_GROUP_INFO,
                    "Fetched a stale storage container info : current = " + scInfo.getRevision()
                        + ", fetched = " + endpoint.getRevision() + ""));
            return;
        }
        // we got the updated location
        List<Endpoint> readEndpoints =
            Lists.newArrayListWithExpectedSize(1 + endpoint.getRoEndpointCount());
        readEndpoints.add(endpoint.getRwEndpoint());
        readEndpoints.addAll(endpoint.getRoEndpointList());
        scInfo = StorageContainerInfo.of(
            scId,
            endpoint.getRevision(),
            endpoint.getRwEndpoint(),
            readEndpoints);
        // get the channel from channel manager (if it doesn't exist create one)
        StorageServerChannel serverChannel = channelManager.getOrCreateChannel(endpoint.getRwEndpoint());
        if (null == serverChannel) {
            log.info("No channel found/created for range server {}. The channel manager must be shutting down."
                + " Stop the process of fetching storage container ({}).", endpoint.getRwEndpoint(), scId);
            synchronized (this) {
                rsChannelFuture.completeExceptionally(
                    new ObjectClosedException("StorageServerChannelManager is closed"));
            }
            return;
        }

        // intercept the storage server channel with additional sc metadata
        StorageServerChannel interceptedChannel = serverChannel.intercept(scId);

        // update the future
        synchronized (this) {
            rsChannelFuture.complete(interceptedChannel);
        }
    }

}
