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
package org.apache.bookkeeper.server.http.service;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;

/**
 * HttpEndpointService that handle Bookkeeper Configuration related http request.
 */
public class ConfigurationService implements HttpEndpointService {

    protected ServerConfiguration conf;

    public ConfigurationService(ServerConfiguration conf) {
        checkNotNull(conf);
        this.conf = conf;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        // GET
        if (HttpServer.Method.GET == request.getMethod()) {
            String jsonResponse = conf.asJson();
            response.setBody(jsonResponse);
            return response;
        } else if (HttpServer.Method.PUT == request.getMethod()) {
            String requestBody = request.getBody();
            if (null == requestBody) {
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("Request body not found. should contains k-v pairs");
                return response;
            }
            @SuppressWarnings("unchecked")
            HashMap<String, Object> configMap = JsonUtil.fromJson(requestBody, HashMap.class);
            for (Map.Entry<String, Object> entry: configMap.entrySet()) {
                conf.setProperty(entry.getKey(), entry.getValue());
            }

            response.setCode(HttpServer.StatusCode.OK);
            response.setBody("Success set server config.");
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Request body not found. should contains k-v pairs");
            return response;
        }

    }
}
