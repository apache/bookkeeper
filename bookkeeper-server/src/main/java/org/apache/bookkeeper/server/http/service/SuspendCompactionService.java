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
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.proto.BookieServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuspendCompactionService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(SuspendCompactionService.class);

    protected BookieServer bookieServer;

    public SuspendCompactionService(BookieServer bookieServer) {
        checkNotNull(bookieServer);
        this.bookieServer = bookieServer;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.PUT == request.getMethod()) {
            String requestBody = request.getBody();
            if (null == requestBody) {
                return new HttpServiceResponse("Empty request body", HttpServer.StatusCode.BAD_REQUEST);
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> configMap = JsonUtil.fromJson(requestBody, HashMap.class);
                Boolean suspendMajor = (Boolean) configMap.get("suspendMajor");
                Boolean suspendMinor = (Boolean) configMap.get("suspendMinor");
                if (suspendMajor == null && suspendMinor == null) {
                    return new HttpServiceResponse("No suspendMajor or suspendMinor params found",
                            HttpServer.StatusCode.BAD_REQUEST);
                }
                String output = "";
                if (suspendMajor != null  && suspendMajor) {
                    output = "Suspend majorGC on BookieServer: " + bookieServer.toString();
                    bookieServer.getBookie().getLedgerStorage().suspendMajorGC();
                }
                if (suspendMinor != null && suspendMinor) {
                    output += ", Suspend minorGC on BookieServer: " + bookieServer.toString();
                    bookieServer.getBookie().getLedgerStorage().suspendMinorGC();
                }
                String jsonResponse = JsonUtil.toJson(output);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("output body:" + jsonResponse);
                }
                response.setBody(jsonResponse);
                response.setCode(HttpServer.StatusCode.OK);
                return response;
            }
        } else if (HttpServer.Method.GET == request.getMethod()) {
            boolean isMajorGcSuspend = bookieServer.getBookie().getLedgerStorage().isMajorGcSuspended();
            boolean isMinorGcSuspend = bookieServer.getBookie().getLedgerStorage().isMinorGcSuspended();
            Map<String, String> output = new HashMap<>();
            output.put("isMajorGcSuspended", Boolean.toString(isMajorGcSuspend));
            output.put("isMinorGcSuspended", Boolean.toString(isMinorGcSuspend));
            String jsonResponse = JsonUtil.toJson(output);
            if (LOG.isDebugEnabled()) {
                LOG.debug("output body:" + jsonResponse);
            }
            response.setBody(jsonResponse);
            response.setCode(HttpServer.StatusCode.OK);
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be PUT to suspend major or minor compaction, "
                    + "Or GET to get compaction state.");
            return response;
        }
    }
}
