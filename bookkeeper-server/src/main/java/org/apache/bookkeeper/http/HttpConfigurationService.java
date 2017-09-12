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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.service.HttpService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.util.JsonUtil;

/**
 * HttpService that handle Bookkeeper Configuration related http request.
 */
public class HttpConfigurationService implements HttpService {

    protected ServerConfiguration conf;

    public HttpConfigurationService(ServerConfiguration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        Map<String, Object> configMap = toMap(conf);
        String jsonResponse = JsonUtil.toJson(configMap);
        response.setBody(jsonResponse);
        return response;
    }

    private Map<String, Object> toMap(ServerConfiguration conf) {
        Map<String, Object> configMap = new HashMap<>();
        Iterator iterator = conf.getKeys();
        while (iterator.hasNext()) {
            String key = iterator.next().toString();
            Object property = conf.getProperty(key);
            if (property != null) {
                configMap.put(key, property.toString());
            }
        }
        return configMap;
    }
}
