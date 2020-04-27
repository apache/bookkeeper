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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Allows a component to publish information about
 * the services it implements, the endpoints it exposes
 * and other useful information for management tools and client.
 */
@Slf4j
public class ComponentInfoPublisher {

    private final Map<String, String> properties = new ConcurrentHashMap<>();
    private final Map<String, EndpointInfo> endpoints = new ConcurrentHashMap<>();

    /**
     * Endpoint information.
     */
    public static final class EndpointInfo {

        private final String id;
        private final int port;
        private final String host;
        private final String protocol;
        private final List<String> auth;
        private final List<String> extensions;

        public EndpointInfo(String id, int port, String host, String protocol,
                            List<String> auth, List<String> extensions) {
            this.id = id;
            this.port = port;
            this.host = host;
            this.protocol = protocol;
            this.auth = auth == null ? Collections.emptyList() : Collections.unmodifiableList(auth);
            this.extensions = extensions == null ? Collections.emptyList() : Collections.unmodifiableList(extensions);
        }

        public String getId() {
            return id;
        }

        public int getPort() {
            return port;
        }

        public String getHost() {
            return host;
        }

        public String getProtocol() {
            return protocol;
        }

        public List<String> getAuth() {
            return auth;
        }

        public List<String> getExtensions() {
            return extensions;
        }

        @Override
        public String toString() {
            return "EndpointInfo{" + "id=" + id + ", port=" + port + ", host=" + host + ", protocol=" + protocol + ", "
                    + "auth=" + auth + ", extensions=" + extensions + '}';
        }

    }

    private volatile boolean startupFinished;

    /**
     * Publish an information about the system, like an endpoint address.
     *
     * @param key the key
     * @param value the value, null values are not allowed.
     */
    public void publishProperty(String key, String value) {
        if (log.isDebugEnabled()) {
            log.debug("publish {}={}", key, value);
        }
        if (startupFinished) {
            throw new IllegalStateException("Server already started, cannot publish " + key);
        }
        Objects.requireNonNull(key);
        Objects.requireNonNull(value, "Value for " + key + " cannot be null");

        properties.put(key, value);
    }

    public void publishEndpoint(EndpointInfo endpoint) {
        if (log.isDebugEnabled()) {
            log.debug("publishEndpoint {} on {}", endpoint, this);
        }
        EndpointInfo exists = endpoints.put(endpoint.id, endpoint);
        if (exists != null) {
            throw new IllegalStateException("An endpoint with id " + endpoint.id
                    + " has already been published: " + exists);
        }
    }

    public Map<String, String> getProperties() {
        if (!startupFinished) {
            throw new IllegalStateException("Startup not yet finished");
        }
        return Collections.unmodifiableMap(properties);
    }

    public Map<String, EndpointInfo> getEndpoints() {
        if (!startupFinished) {
            throw new IllegalStateException("Startup not yet finished");
        }
        return Collections.unmodifiableMap(endpoints);
    }

    /**
     * Called by the framework to signal that preparation of startup is done,
     * so we have gathered all of the available information.
     */
    public void startupFinished() {
        startupFinished = true;
    }

}
