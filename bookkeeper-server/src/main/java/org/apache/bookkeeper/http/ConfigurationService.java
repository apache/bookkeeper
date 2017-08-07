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
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.service.Service;
import org.apache.bookkeeper.http.service.ServiceRequest;
import org.apache.bookkeeper.http.service.ServiceResponse;
import org.apache.bookkeeper.util.JsonUtil;

/**
 * Service that handle Bookkeeper Configuration related http request.
 */
public class ConfigurationService implements Service {

    protected ServerConfiguration conf;

    public ConfigurationService(ServerConfiguration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    @Override
    public ServiceResponse handle(ServiceRequest request) throws Exception {
        ServiceResponse response = new ServiceResponse();
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
