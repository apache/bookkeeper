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
package org.apache.bookkeeper.discover;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Information about services exposed by a Bookie.
 */
public final class BookieServiceInfo {

    /**
     * Default empty implementation.
     */
    public static final BookieServiceInfo EMPTY = new BookieServiceInfo(
            Collections.emptyMap(),
            Collections.emptyList()
    );

    /**
     * Default empty implementation.
     */
    public static final Supplier<BookieServiceInfo> NO_INFO = () -> EMPTY;

    private Map<String, String> properties;
    private List<Endpoint> endpoints;

    public BookieServiceInfo(Map<String, String> properties, List<Endpoint> endpoints) {
        this.properties = Collections.unmodifiableMap(properties);
        this.endpoints = Collections.unmodifiableList(endpoints);
    }

    public BookieServiceInfo() {
        this(Collections.emptyMap(), Collections.emptyList());
    }

    /**
     * Unmodifiable map with bookie wide information.
     *
     * @return the map
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Unmodifieable structure with the list of exposed endpoints.
     *
     * @return the list.
     */
    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setEndpoints(List<Endpoint> endpoints) {
        this.endpoints = endpoints;
    }

    /**
     * Information about an endpoint.
     */
    public static final class Endpoint {

        private String id;
        private int port;
        private String host;
        private String protocol;
        private List<String> auth;
        private List<String> extensions;

        public Endpoint(String id, int port, String host, String protocol, List<String> auth, List<String> extensions) {
            this.id = id;
            this.port = port;
            this.host = host;
            this.protocol = protocol;
            this.auth = auth;
            this.extensions = extensions;
        }

        public Endpoint() {
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getProtocol() {
            return protocol;
        }

        public void setProtocol(String protocol) {
            this.protocol = protocol;
        }

        public List<String> getAuth() {
            return auth;
        }

        public void setAuth(List<String> auth) {
            this.auth = auth;
        }

        public List<String> getExtensions() {
            return extensions;
        }

        public void setExtensions(List<String> extensions) {
            this.extensions = extensions;
        }

        @Override
        public String toString() {
            return "EndpointInfo{" + "id=" + id + ", port=" + port + ", host=" + host + ", protocol=" + protocol + ", "
                    + "auth=" + auth + ", extensions=" + extensions + '}';
        }

    }

    @Override
    public String toString() {
        return "BookieServiceInfo{" + "properties=" + properties + ", endpoints=" + endpoints + '}';
    }

}
