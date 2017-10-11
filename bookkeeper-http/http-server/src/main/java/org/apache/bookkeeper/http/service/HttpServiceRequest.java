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

import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.http.HttpServer;

/**
 * A wrapper class that wrap a http request into a class which
 * can then be passed into the service.
 */
public class HttpServiceRequest {
    private String body;
    private HttpServer.Method method = HttpServer.Method.GET;
    private Map<String, String> params = new HashMap<>();

    public HttpServiceRequest() {}

    public HttpServiceRequest(String body, HttpServer.Method method, Map<String, String> params) {
        this.body = body;
        this.method = method;
        this.params = params;
    }

    public String getBody() {
        return body;
    }

    public HttpServiceRequest setBody(String body) {
        this.body = body;
        return this;
    }

    public HttpServer.Method getMethod() {
        return method;
    }

    public HttpServiceRequest setMethod(HttpServer.Method method) {
        this.method = method;
        return this;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public HttpServiceRequest setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }
}
