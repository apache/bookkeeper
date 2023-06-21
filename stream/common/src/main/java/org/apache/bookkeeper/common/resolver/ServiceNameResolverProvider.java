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

package org.apache.bookkeeper.common.resolver;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.internal.DnsNameResolverProvider;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;

/**
 * An implementation of {@link NameResolverProvider} that provides {@link NameResolver}s
 * to resolve {@link org.apache.bookkeeper.common.net.ServiceURI}.
 */
@Slf4j
public final class ServiceNameResolverProvider extends NameResolverFactoryProvider {

    private final NameResolverProvider dnsProvider;
    private final Resource<ExecutorService> executorResource;

    public ServiceNameResolverProvider() {
        this.dnsProvider = new DnsNameResolverProvider();
        this.executorResource = new Resource<ExecutorService>() {
            @Override
            public ExecutorService create() {
                return Executors.newSingleThreadScheduledExecutor();
            }

            @Override
            public void close(ExecutorService instance) {
                instance.shutdown();
            }
        };
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 10;
    }

    @Nullable
    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args params) {
        ServiceURI serviceURI;
        try {
            serviceURI = ServiceURI.create(targetUri);
        } catch (NullPointerException | IllegalArgumentException e) {
            // invalid uri here, so return null to allow grpc to use other name resolvers
            log.info("ServiceNameResolverProvider doesn't know how to resolve {} : cause {}",
                targetUri, e.getMessage());
            return null;
        }

        if (null == serviceURI.getServiceName()
            || ServiceURI.SERVICE_BK.equals(serviceURI.getServiceName())) {

            String[] hosts = serviceURI.getServiceHosts();
            if (hosts.length == 0) {
                // no host is find, so return null to let grpc choose other resolver.
                return null;
            } else if (hosts.length == 1) {
                // create a dns name resolver
                URI dnsUri = URI.create("dns:///" + hosts[0]);
                return dnsProvider.newNameResolver(dnsUri, params);
            } else {
                // create a static resolver taking the list of servers.
                List<String> hostList = new ArrayList<>();
                for (String host : hosts) {
                    hostList.add(host);
                }
                List<URI> hostUris = Lists.transform(
                    hostList,
                    new Function<String, URI>() {
                        @Nullable
                        @Override
                        public URI apply(@Nullable String host) {
                            return URI.create("//" + host);
                        }
                    }
                );

                return new StaticNameResolver(
                    "static",
                    executorResource,
                    hostUris);
            }
        } else {
            return null;
        }
    }

    @Override
    public String getDefaultScheme() {
        return ServiceURI.SERVICE_BK;
    }

    @Override
    public NameResolver.Factory toFactory() {
        return new NameResolverProviderFactory(Lists.newArrayList(this));
    }

}
