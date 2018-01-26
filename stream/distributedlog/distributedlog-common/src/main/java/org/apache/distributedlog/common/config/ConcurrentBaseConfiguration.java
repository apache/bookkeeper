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
package org.apache.distributedlog.common.config;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.configuration.AbstractConfiguration;

/**
 * Configuration view built on concurrent hash map for fast thread-safe access.
 * Notes:
 * 1. Multi-property list aggregation will not work in this class. I.e. commons config
 * normally combines all properties with the same key into one list property automatically.
 * This class simply overwrites any existing mapping.
 */
public class ConcurrentBaseConfiguration extends AbstractConfiguration {

    private final ConcurrentHashMap<String, Object> map;

    public ConcurrentBaseConfiguration() {
        this.map = new ConcurrentHashMap<String, Object>();
    }

    @Override
    protected void addPropertyDirect(String key, Object value) {
        checkNotNull(value);
        map.put(key, value);
    }

    @Override
    public Object getProperty(String key) {
        return map.get(key);
    }

    @Override
    public Iterator getKeys() {
        return map.keySet().iterator();
    }

    @Override
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    protected void clearPropertyDirect(String key) {
        map.remove(key);
    }
}
