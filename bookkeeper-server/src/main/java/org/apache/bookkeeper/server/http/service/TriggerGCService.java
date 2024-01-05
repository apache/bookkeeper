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
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle force trigger GC requests.
 *
 * <p>The PUT method will force trigger GC on current bookie, and make GC run at backend.
 *
 * <p>The GET method will get the force triggered GC running or not.
 * Output would be like:
 *        {
 *           "is_in_force_gc" : "false"
 *        }
 */
public class TriggerGCService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(TriggerGCService.class);

    protected ServerConfiguration conf;
    protected BookieServer bookieServer;

    public TriggerGCService(ServerConfiguration conf, BookieServer bookieServer) {
        checkNotNull(conf);
        checkNotNull(bookieServer);
        this.conf = conf;
        this.bookieServer = bookieServer;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        try {
            if (HttpServer.Method.PUT == request.getMethod()) {
                String requestBody = request.getBody();
                if (StringUtils.isBlank(requestBody)) {
                    bookieServer.getBookie().getLedgerStorage().forceGC();
                } else {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> configMap = JsonUtil.fromJson(requestBody, HashMap.class);
                    LedgerStorage ledgerStorage = bookieServer.getBookie().getLedgerStorage();
                    boolean forceMajor = !ledgerStorage.isMajorGcSuspended();
                    boolean forceMinor = !ledgerStorage.isMinorGcSuspended();

                    forceMajor = Boolean.parseBoolean(configMap.getOrDefault("forceMajor", forceMajor).toString());
                    forceMinor = Boolean.parseBoolean(configMap.getOrDefault("forceMinor", forceMinor).toString());
                    ledgerStorage.forceGC(forceMajor, forceMinor);
                }

                String output = "Triggered GC on BookieServer: " + bookieServer.getBookieId();
                String jsonResponse = JsonUtil.toJson(output);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("output body:" + jsonResponse);
                }
                response.setBody(jsonResponse);
                response.setCode(HttpServer.StatusCode.OK);
                return response;
            } else if (HttpServer.Method.GET == request.getMethod()) {
                Boolean isInForceGC = bookieServer.getBookie().getLedgerStorage().isInForceGC();
                Pair<String, String> output = Pair.of("is_in_force_gc", isInForceGC.toString());
                String jsonResponse = JsonUtil.toJson(output);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("output body:" + jsonResponse);
                }
                response.setBody(jsonResponse);
                response.setCode(HttpServer.StatusCode.OK);
                return response;
            } else {
                response.setCode(HttpServer.StatusCode.METHOD_NOT_ALLOWED);
                response.setBody("Not allowed method. Should be PUT to trigger GC, Or GET to get Force GC state.");
                return response;
            }
        } catch (Exception e) {
            LOG.error("Failed to handle the request, method: {}, body: {} ", request.getMethod(), request.getBody(), e);
            response.setCode(HttpServer.StatusCode.BAD_REQUEST);
            response.setBody("Failed to handle the request, exception: " + e.getMessage());
            return response;
        }
    }
}
