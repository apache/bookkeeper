/*
 *
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
 *
 */
package org.apache.bookkeeper.feature;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang.StringUtils;

/**
 * Cacheable Feature Provider.
 */
public abstract class CacheableFeatureProvider<T extends Feature> implements FeatureProvider {

    protected final String scope;
    protected final ConcurrentMap<String, FeatureProvider> scopes =
            new ConcurrentHashMap<String, FeatureProvider>();
    protected final ConcurrentMap<String, T> features =
            new ConcurrentHashMap<String, T>();

    protected CacheableFeatureProvider(String scope) {
        this.scope = scope;
    }

    protected String makeName(String name) {
        if (StringUtils.isBlank(scope)) {
            return name;
        } else {
            return scope + "." + name;
        }
    }

    @Override
    public T getFeature(String name) {
        T feature = features.get(name);
        if (null == feature) {
            T newFeature = makeFeature(makeName(name));
            T oldFeature = features.putIfAbsent(name, newFeature);
            if (null == oldFeature) {
                feature = newFeature;
            } else {
                feature = oldFeature;
            }
        }
        return feature;
    }

    protected abstract T makeFeature(String featureName);

    @Override
    public FeatureProvider scope(String name) {
        FeatureProvider provider = scopes.get(name);
        if (null == provider) {
            FeatureProvider newProvider = makeProvider(makeName(name));
            FeatureProvider oldProvider = scopes.putIfAbsent(name, newProvider);
            if (null == oldProvider) {
                provider = newProvider;
            } else {
                provider = oldProvider;
            }
        }
        return provider;
    }

    protected abstract FeatureProvider makeProvider(String fullScopeName);
}
