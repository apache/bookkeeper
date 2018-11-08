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

package org.apache.bookkeeper.clients.config;

import java.util.Optional;
import org.apache.bookkeeper.clients.resolver.EndpointResolver;
import org.apache.bookkeeper.clients.utils.ClientConstants;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.stats.StatsLogger;
import org.inferred.freebuilder.FreeBuilder;

/**
 * Settings to configure a stream storage client.
 */
@FreeBuilder
public interface StorageClientSettings {

    /**
     * Returns the number of worker threads in the core scheduler used by the client.
     *
     * @return the number of worker threads.
     */
    int numWorkerThreads();

    /**
     * Returns the service uri that storage client should talk to.
     *
     * @return service uri
     */
    String serviceUri();

    /**
     * Return the endpoint resolver for resolving individual endpoints.
     *
     * <p>The default resolver is an identity resolver.
     *
     * @return the endpoint resolver for resolving endpoints.
     */
    EndpointResolver endpointResolver();

    /**
     * Use of a plaintext connection to the server. By default a secure connection mechanism
     * such as TLS will be used.
     *
     * <p>Should only be used for testing or for APIs where the use of such API or the data
     * exchanged is not sensitive.
     *
     * @return true if use a plaintext connection to the server, otherwise false.
     */
    boolean usePlaintext();

    /**
     * Configure the client name.
     *
     * @return client name.
     */
    Optional<String> clientName();

    /**
     * Configure a stats logger to collect stats exposed by this client.
     *
     * @return stats logger.
     */
    Optional<StatsLogger> statsLogger();

    /**
     * Configure a backoff policy for the client.
     *
     * <p>There are a few default backoff policies defined in {@link org.apache.bookkeeper.common.util.Backoff}.
     *
     * @return backoff policy provider
     */
    Backoff.Policy backoffPolicy();

    /**
     * Configure whether to enable server side routing or not.
     *
     * <p>By default, the client implementation will does client side routing, which will talk to storage containers
     * directly, however sometimes if you can simply expose storage containers addresses due to network security
     * constraints, you can enable server side routing. in server side routing mode, the clients simply make
     * grpc calls to any storage container, those storage containers will route the requests accordingly to the
     * right storage container. In this mode, the storage containers act as grpc proxies.
     *
     * @return flag whether to enable server side routing or not.
     */
    boolean enableServerSideRouting();

    /**
     * Builder of {@link StorageClientSettings} instances.
     */
    class Builder extends StorageClientSettings_Builder {

        Builder() {
            numWorkerThreads(Runtime.getRuntime().availableProcessors());
            usePlaintext(true);
            backoffPolicy(ClientConstants.DEFAULT_INFINIT_BACKOFF_POLICY);
            endpointResolver(EndpointResolver.identity());
            enableServerSideRouting(false);
        }

        @Override
        public StorageClientSettings build() {
            StorageClientSettings settings = super.build();

            // create a service uri to ensure the service uri is valid
            ServiceURI.create(serviceUri());

            return settings;
        }
    }

    static Builder newBuilder() {
        // builder with default values
        return new Builder();
    }

}
