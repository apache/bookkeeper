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

package org.apache.bookkeeper.clients.resolver;

import org.apache.bookkeeper.stream.proto.common.Endpoint;

/**
 * Resolve an endpoint to another endpoint.
 *
 * <p>The resolver can be used for resolving the right ip address for an advertised endpoint. It is typically useful
 * in dockerized integration tests, where the test clients are typically outside of the docker network.
 */
public interface EndpointResolver {

    /**
     * Returns a resolver that always returns its input endpoint.
     *
     * @return a function that always returns its input endpoint
     */
    static EndpointResolver identity() {
        return endpoint -> endpoint;
    }

    /**
     * Resolve <tt>endpoint</tt> to another endpoint.
     *
     * @param endpoint endpoint to resolve
     * @return the resolved endpoint.
     */
    Endpoint resolve(Endpoint endpoint);

}
