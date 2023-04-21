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

package org.apache.bookkeeper.common.grpc.proxy;

import static com.google.common.base.Preconditions.checkNotNull;

import io.grpc.CallOptions;
import io.grpc.HandlerRegistry;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCallHandler;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.bookkeeper.common.grpc.netty.IdentityInputStreamMarshaller;

/**
 * Registry for proxying grpc services.
 */
public class ProxyHandlerRegistry extends HandlerRegistry {

    private final Map<String, ServerMethodDefinition<?, ?>> methods;

    private ProxyHandlerRegistry(Map<String, ServerMethodDefinition<?, ?>> methods) {
        this.methods = methods;
    }

    @Nullable
    @Override
    public ServerMethodDefinition<?, ?> lookupMethod(String methodName,
                                                     @Nullable String authority) {
        return methods.get(methodName);
    }

    private static ServerMethodDefinition<?, ?> createProxyServerMethodDefinition(
            MethodDescriptor<?, ?> methodDesc,
            ServerCallHandler<InputStream, InputStream> handler) {
        MethodDescriptor<InputStream, InputStream> methodDescriptor = MethodDescriptor.newBuilder(
            IdentityInputStreamMarshaller.of(), IdentityInputStreamMarshaller.of())
            .setFullMethodName(methodDesc.getFullMethodName())
            .setType(methodDesc.getType())
            .setIdempotent(methodDesc.isIdempotent())
            .setSafe(methodDesc.isSafe())
            .build();
        return ServerMethodDefinition.create(methodDescriptor, handler);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder to build the handler registry.
     */
    public static class Builder {

        // store per-service first, to make sure services are added/replaced atomically.
        private final HashMap<String, ServerServiceDefinition> services =
            new LinkedHashMap<>();
        private ChannelFinder finder;

        /**
         * Add the service to this grpc handler registry.
         *
         * @param service grpc service definition
         * @return registry builder
         */
        public Builder addService(ServerServiceDefinition service) {
            services.put(
                service.getServiceDescriptor().getName(),
                service);
            return this;
        }

        /**
         * Registered a channel finder for proxying server calls.
         *
         * @param finder channel finder
         * @return registry builder
         */
        public Builder setChannelFinder(ChannelFinder finder) {
            this.finder = finder;
            return this;
        }

        /**
         * Build the proxy handler registry.
         *
         * @return registry builder
         */
        public ProxyHandlerRegistry build() {
            checkNotNull(finder, "No channel finder defined");

            ProxyServerCallHandler<InputStream, InputStream> proxyHandler =
                new ProxyServerCallHandler<>(finder, CallOptions.DEFAULT);

            Map<String, ServerMethodDefinition<?, ?>> methods = new HashMap<>();
            for (ServerServiceDefinition service : services.values()) {
                for (ServerMethodDefinition<?, ?> method : service.getMethods()) {
                    String methodName = method.getMethodDescriptor().getFullMethodName();
                    methods.put(
                        methodName,
                        createProxyServerMethodDefinition(
                            method.getMethodDescriptor(),
                            proxyHandler)
                    );
                }
            }
            return new ProxyHandlerRegistry(
                Collections.unmodifiableMap(methods));
        }

    }

}
