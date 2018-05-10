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
package org.apache.bookkeeper.clients.impl.internal;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.impl.internal.api.RootRangeClient;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.Retries;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * A root range client wrapper with retries.
 */
@Slf4j
class RootRangeClientImplWithRetries implements RootRangeClient {

    @VisibleForTesting
    static final Predicate<Throwable> ROOT_RANGE_CLIENT_RETRY_PREDICATE =
        cause -> shouldRetryOnException(cause);

    private static boolean shouldRetryOnException(Throwable cause) {
        if (cause instanceof StatusRuntimeException || cause instanceof StatusException) {
            Status status;
            if (cause instanceof StatusException) {
                status = ((StatusException) cause).getStatus();
            } else {
                status = ((StatusRuntimeException) cause).getStatus();
            }
            switch (status.getCode()) {
                case INVALID_ARGUMENT:
                case ALREADY_EXISTS:
                case PERMISSION_DENIED:
                case UNAUTHENTICATED:
                    return false;
                default:
                    return true;
            }
        } else if (cause instanceof RuntimeException) {
            return false;
        } else {
            // storage level exceptions
            return false;
        }
    }

    private final RootRangeClient client;
    private final Backoff.Policy backoffPolicy;
    private final OrderedScheduler scheduler;

    RootRangeClientImplWithRetries(RootRangeClient client,
                                   Backoff.Policy backoffPolicy,
                                   OrderedScheduler scheduler) {
        this.client = client;
        this.backoffPolicy = backoffPolicy;
        this.scheduler = scheduler;
    }

    private <T> CompletableFuture<T> runRpcWithRetries(
            Supplier<CompletableFuture<T>> futureSupplier) {
        return Retries.run(
            backoffPolicy.toBackoffs(),
            ROOT_RANGE_CLIENT_RETRY_PREDICATE,
            futureSupplier,
            scheduler);
    }

    @Override
    public CompletableFuture<NamespaceProperties> createNamespace(String namespace,
                                                                  NamespaceConfiguration nsConf) {
        return runRpcWithRetries(() -> client.createNamespace(namespace, nsConf));
    }

    @Override
    public CompletableFuture<Boolean> deleteNamespace(String namespace) {
        return runRpcWithRetries(() -> client.deleteNamespace(namespace));
    }

    @Override
    public CompletableFuture<NamespaceProperties> getNamespace(String namespace) {
        return runRpcWithRetries(() -> client.getNamespace(namespace));
    }

    @Override
    public CompletableFuture<StreamProperties> createStream(String nsName,
                                                            String streamName,
                                                            StreamConfiguration streamConf) {
        return runRpcWithRetries(() ->
            client.createStream(nsName, streamName, streamConf));
    }

    @Override
    public CompletableFuture<Boolean> deleteStream(String nsName, String streamName) {
        return runRpcWithRetries(() ->
            client.deleteStream(nsName, streamName));
    }

    @Override
    public CompletableFuture<StreamProperties> getStream(String nsName, String streamName) {
        return runRpcWithRetries(() ->
            client.getStream(nsName, streamName));
    }

    @Override
    public CompletableFuture<StreamProperties> getStream(long streamId) {
        return runRpcWithRetries(() ->
            client.getStream(streamId));
    }
}
