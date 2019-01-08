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

package org.apache.bookkeeper.common.resolver;

import static com.google.common.base.Preconditions.checkState;

import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import java.net.URI;
import java.util.List;

/**
 * A {@link io.grpc.NameResolverProvider} based {@link NameResolver.Factory}.
 */
public class NameResolverProviderFactory extends NameResolver.Factory {

    private final List<NameResolverProvider> providers;

    public NameResolverProviderFactory(List<NameResolverProvider> providers) {
        this.providers = providers;
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, Attributes params) {
        checkForProviders();
        for (NameResolverProvider provider : providers) {
            NameResolver resolver = provider.newNameResolver(targetUri, params);
            if (resolver != null) {
                return resolver;
            }
        }
        return null;
    }

    @Override
    public String getDefaultScheme() {
        checkForProviders();
        return providers.get(0).getDefaultScheme();
    }

    private void checkForProviders() {
        checkState(!providers.isEmpty(),
            "No NameResolverProviders found. Please check your configuration");
    }

}
