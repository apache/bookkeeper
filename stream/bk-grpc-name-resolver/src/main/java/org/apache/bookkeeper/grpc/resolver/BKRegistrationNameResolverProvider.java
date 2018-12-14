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

package org.apache.bookkeeper.grpc.resolver;

import com.google.common.collect.Lists;
import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import java.net.URI;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.common.resolver.NameResolverFactoryProvider;
import org.apache.bookkeeper.common.resolver.NameResolverProviderFactory;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;

/**
 * An implementation of {@link NameResolverProvider} that provides {@link io.grpc.NameResolver}s
 * to resolve servers registered using bookkeeper registration library.
 */
@Slf4j
public class BKRegistrationNameResolverProvider extends NameResolverFactoryProvider {

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 100;
    }

    @Nullable
    @Override
    public NameResolver newNameResolver(URI targetUri, Attributes params) {
        ServiceURI serviceURI;
        try {
            serviceURI = ServiceURI.create(targetUri);
        } catch (NullPointerException | IllegalArgumentException e) {
            // invalid uri here, so return null to allow grpc to use other name resolvers
            log.info("BKRegistrationNameResolverProvider doesn't know how to resolve {} : cause {}",
                targetUri, e.getMessage());
            return null;
        }

        MetadataClientDriver clientDriver;
        try {
            clientDriver = MetadataDrivers.getClientDriver(serviceURI.getUri());
            return new BKRegistrationNameResolver(clientDriver, serviceURI.getUri());
        } catch (IllegalArgumentException iae) {
            log.error("Unknown service uri : {}", serviceURI, iae);
            return null;
        }
    }

    @Override
    public String getDefaultScheme() {
        return ServiceURI.SERVICE_ZK;
    }

    @Override
    public NameResolver.Factory toFactory() {
        return new NameResolverProviderFactory(Lists.newArrayList(this));
    }
}
