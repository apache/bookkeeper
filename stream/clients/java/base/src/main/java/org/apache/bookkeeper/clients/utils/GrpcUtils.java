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

import static org.apache.bookkeeper.clients.utils.ClientConstants.TOKEN;

import io.grpc.CallCredentials2;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Grpc related utils.
 */
public final class GrpcUtils {

    private GrpcUtils() {
    }

    /**
     * Configure a grpc stub with optional credential token.
     *
     * @param stub  grpc stub
     * @param token credential token
     * @param <T>
     * @return a configured grpc stub.
     */
    public static <T extends AbstractStub<T>> T configureGrpcStub(T stub, Optional<String> token) {
        return token.map(t -> {
            Metadata metadata = new Metadata();
            Metadata.Key<String> tokenKey = Metadata.Key.of(TOKEN, Metadata.ASCII_STRING_MARSHALLER);
            metadata.put(tokenKey, t);
            CallCredentials2 callCredentials = new CallCredentials2() {
                @Override
                public void applyRequestMetadata(RequestInfo requestInfo,
                                                 Executor appExecutor,
                                                 MetadataApplier applier) {
                    applier.apply(metadata);
                }

                @Override
                public void thisUsesUnstableApi() {
                    // no-op;
                }
            };
            return stub.withCallCredentials(callCredentials);
        }).orElse(stub);
    }

    /**
     * Process rpc exception.
     *
     * @param rpcCause rpc cause.
     * @param future   future to complete exceptionally
     */
    public static <T> void processRpcException(Throwable rpcCause, CompletableFuture<T> future) {
        future.completeExceptionally(rpcCause);
    }

}
