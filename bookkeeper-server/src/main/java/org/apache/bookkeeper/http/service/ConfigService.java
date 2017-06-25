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

import java.util.Iterator;
import java.util.List;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.json.JSONObject;

public class ConfigService {
    private ServerConfiguration conf;

    public ConfigService(ServerConfiguration conf) {
        this.conf = conf;
    }

    public String getServerConfiguration(){
        JSONObject response = new JSONObject();
        JSONObject config = new JSONObject();
        Iterator iterator = conf.getKeys();
        while (iterator.hasNext()) {
            String key = iterator.next().toString();
            List values = conf.getList(key);
            if (values.size() >= 0) {
                config.put(key, values.get(0));
            }
        }
        response.put("server_config", config);
        return response.toString(2);
    }
}
