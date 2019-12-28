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

package org.apache.bookkeeper.common.component;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Allows a component to publish information about
 * the services it implements, the endpoints it exposes
 * and other useful information for management tools and client.
 *
 */
public class ComponentInfoPublisher {

    private final Map<String, String> allInfo = new ConcurrentHashMap<>();
    private boolean startupFinished;
    
    /**
     * Publish an information about the system, like an endpoint address.
     *
     * @param key the key
     * @param value  the value, null values are not allowed.
     */
    public void publish(String key, String value) {
        if (startupFinished) {
            throw new IllegalStateException("Server already started, cannot publish "+key);
        }
        Objects.requireNonNull(key);
        Objects.requireNonNull(value, "Value for "+key+" cannot be null");
        
        allInfo.put(key, value);
    }
    
    public Map<String, String> getInfo() {
        if (!startupFinished) {
            throw new IllegalStateException("Startup not yet finished");
        }
        return Collections.unmodifiableMap(allInfo);
    }
    
    public void startupFinished() {
        startupFinished = true;
    }
    
}
