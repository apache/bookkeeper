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

package org.apache.bookkeeper.clients.utils;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.Backoff.Policy;

/**
 * A process for processing rpc request on storage container channel.
 */
@Slf4j
public abstract class ListenableFutureRpcProcessor<RequestT, ResponseT, ResultT>
    implements BiConsumer<StorageServerChannel, Throwable>,
    FutureCallback<ResponseT>,
    Runnable {

    private final StorageContainerChannel scChannel;
    private final Iterator<Long> backoffs;
    private final ScheduledExecutorService executor;
    private final CompletableFuture<ResultT> resultFuture;

    private CompletableFuture<StorageServerChannel> serverChannelFuture = null;

    protected ListenableFutureRpcProcessor(StorageContainerChannel channel,
                                           ScheduledExecutorService executor,
                                           Policy backoffPolicy) {
        this.scChannel = channel;
        this.backoffs = backoffPolicy.toBackoffs().iterator();
        this.resultFuture = FutureUtils.createFuture();
        this.executor = executor;
    }

    /**
     * Create the rpc request for the processor.
     *
     * @return the created rpc request.
     */
    protected abstract RequestT createRequest();

    /**
     * Get the RPC service from the server channel.
     *
     * @return rpc service.
     */
    protected abstract ListenableFuture<ResponseT> sendRPC(StorageServerChannel rsChannel, RequestT requestT);

    /**
     * Process the response and convert it back to a result.
     *
     * @param response response
     * @return the converted result.
     */
    protected abstract ResultT processResponse(ResponseT response) throws Exception;

    public CompletableFuture<ResultT> process() {
        serverChannelFuture = scChannel.getStorageContainerChannelFuture();
        serverChannelFuture.whenCompleteAsync(this, executor);
        return resultFuture;
    }

    @Override
    public void run() {
        process();
    }

    /**
     * Logic on handling channel exceptions.
     *
     * @param storageServerChannel server channel
     * @param cause                exception on establishing channel.
     */
    @Override
    public void accept(StorageServerChannel storageServerChannel, Throwable cause) {
        if (null != cause) {
            // The `StorageContainerChannel` already retry on failures related to server channel,
            // So we don't need to retry here if failed to retrieve a channel to the server
            // that hosts this storage container.
            resultFuture.completeExceptionally(cause);
            return;
        }

        sendRpcToServerChannel(storageServerChannel);
    }

    private void sendRpcToServerChannel(StorageServerChannel rsChannel) {
        RequestT request;
        try {
            request = createRequest();
        } catch (Exception e) {
            // fail to create request
            resultFuture.completeExceptionally(e);
            return;
        }

        Futures.addCallback(
            sendRPC(rsChannel, request),
            this,
            executor);
    }

    @Override
    public void onSuccess(ResponseT result) {
        try {
            resultFuture.complete(processResponse(result));
        } catch (Exception e) {
            resultFuture.completeExceptionally(e);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        Status status = null;
        if (t instanceof StatusRuntimeException) {
            status = ((StatusRuntimeException) t).getStatus();
        } else if (t instanceof StatusException) {
            status = ((StatusException) t).getStatus();
        }

        if (Status.NOT_FOUND == status) {
            // `NOT_FOUND` means storage container is not found. that means:
            //
            // - the container is moved to a different server
            // - the container is not assigned to any servers yet
            // - the container is assigned, but it is still starting up and not ready for serving
            //
            // at either case, we need to reset the storage server channel, so next retry can attempt to re-locate
            // the storage container again
            scChannel.resetStorageServerChannelFuture(serverChannelFuture);
        }

        if (shouldRetryOn(status) && backoffs.hasNext()) {
            long backoffMs = backoffs.next();
            executor.schedule(this, backoffMs, TimeUnit.MILLISECONDS);
        } else {
            resultFuture.completeExceptionally(t);
        }
    }

    protected boolean shouldRetryOn(Status statusCode) {
        return Status.NOT_FOUND == statusCode;
    }
}
