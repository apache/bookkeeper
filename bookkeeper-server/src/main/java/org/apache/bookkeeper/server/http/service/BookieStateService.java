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

import lombok.Data;
import lombok.NoArgsConstructor;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;

/**
 * HttpEndpointService that exposes the current state of the bookie.
 *
 * <p>Get the current bookie status:
 *
 * <pre>
 * <code>
 * {
 *  "running" : true,
 *  "readOnly" : false,
 *  "shuttingDown" : false,
 *  "availableForHighPriorityWrites" : true
 *}
 * </code>
 * </pre>
 */
public class BookieStateService implements HttpEndpointService {

    private final Bookie bookie;

    public BookieStateService(Bookie bookie) {
        this.bookie = checkNotNull(bookie);
    }

    /**
     * POJO definition for the bookie state response.
     */
    @Data
    @NoArgsConstructor
    public static class BookieState {
        private boolean running;
        private boolean readOnly;
        private boolean shuttingDown;
        private boolean availableForHighPriorityWrites;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.GET != request.getMethod()) {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Only support GET method to retrieve bookie state.");
            return response;
        }

        StateManager sm = bookie.getStateManager();
        BookieState bs = new BookieState();
        bs.running = sm.isRunning();
        bs.readOnly = sm.isReadOnly();
        bs.shuttingDown = sm.isShuttingDown();
        bs.availableForHighPriorityWrites = sm.isAvailableForHighPriorityWrites();

        String jsonResponse = JsonUtil.toJson(bs);
        response.setBody(jsonResponse);
        response.setCode(HttpServer.StatusCode.OK);
        return response;
    }
}
