/**
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
package org.apache.bookkeeper.http;

import java.io.IOException;
import org.apache.bookkeeper.http.service.HeartbeatService;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.NullHttpService;

/**
 * HttpEndpointService provider which provide service that do nothing.
 */
public class NullHttpServiceProvider implements HttpServiceProvider {

    private static final NullHttpServiceProvider NULL_HTTP_SERVICE_PROVIDER = new NullHttpServiceProvider();

    static final HttpEndpointService NULL_HTTP_SERVICE = new NullHttpService();

    public static NullHttpServiceProvider getInstance() {
        return NULL_HTTP_SERVICE_PROVIDER;
    }

    @Override
    public HttpEndpointService provideHttpEndpointService(HttpServer.ApiType type) {
        if (type == HttpServer.ApiType.HEARTBEAT) {
            return new HeartbeatService();
        }
        return NULL_HTTP_SERVICE;
    }

    @Override
    public void close() throws IOException {
    }
}
