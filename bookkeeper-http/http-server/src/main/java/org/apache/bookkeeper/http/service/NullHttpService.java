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
package org.apache.bookkeeper.http.service;

import org.apache.bookkeeper.http.HttpServer;

/**
 * HttpEndpointService that return fixed content.
 */
public class NullHttpService implements HttpEndpointService {
    public static final String CONTENT = "NullHttpService\n";

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) {
        if (request.getBody() != null) {
            return new HttpServiceResponse(request.getBody(), HttpServer.StatusCode.OK);
        }
        return new HttpServiceResponse(CONTENT, HttpServer.StatusCode.OK);
    }
}
