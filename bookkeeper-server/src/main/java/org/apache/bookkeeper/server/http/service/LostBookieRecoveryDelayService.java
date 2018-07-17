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

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper lost bookie recovery delay parameter related http request.
 *
 * <p>The GET method will get the value of parameter lostBookieRecoveryDelay,
 * while the PUT method will set the value of parameter lostBookieRecoveryDelay,
 */
public class LostBookieRecoveryDelayService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(LostBookieRecoveryDelayService.class);

    protected ServerConfiguration conf;
    protected BookKeeperAdmin bka;

    public LostBookieRecoveryDelayService(ServerConfiguration conf, BookKeeperAdmin bka) {
        checkNotNull(conf);
        this.conf = conf;
        this.bka = bka;
    }

    /*
     * set/get lostBookieRecoveryDelay.
     */
    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.PUT == request.getMethod()) {
            // request body as {"delay_seconds": <delay_seconds>}
            String requestBody = request.getBody();

            if (requestBody == null) {
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("Null request body for lostBookieRecoveryDelay.");
                return response;
            }

            @SuppressWarnings("unchecked")
            HashMap<String, Integer> configMap = JsonUtil.fromJson(requestBody, HashMap.class);
            if (configMap != null && configMap.containsKey("delay_seconds")) {
                int delaySeconds = configMap.get("delay_seconds");
                bka.setLostBookieRecoveryDelay(delaySeconds);
                response.setCode(HttpServer.StatusCode.OK);
                response.setBody("Success set lostBookieRecoveryDelay to " + delaySeconds);
                return response;
            } else {
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("Request body not contains lostBookieRecoveryDelay.");
                return response;
            }
        } else if (HttpServer.Method.GET == request.getMethod()) {
            try {
                int delaySeconds = bka.getLostBookieRecoveryDelay();
                response.setCode(HttpServer.StatusCode.OK);
                response.setBody("lostBookieRecoveryDelay value: " + delaySeconds);
                LOG.debug("response body:" + response.getBody());
                return response;
            } catch (Exception e) {
                // may get noNode exception
                LOG.error("Exception occurred while getting lost bookie recovery delay", e);
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("Exception when get lostBookieRecoveryDelay." + e.getMessage());
                return response;
            }
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be PUT method");
            return response;
        }
    }
}
