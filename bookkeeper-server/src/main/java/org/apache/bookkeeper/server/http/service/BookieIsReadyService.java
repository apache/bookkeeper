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

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;

/**
 * HttpEndpointService that returns 200 if the bookie is ready.
 */
public class BookieIsReadyService implements HttpEndpointService {

    private final Bookie bookie;

    public BookieIsReadyService(Bookie bookie) {
        this.bookie = checkNotNull(bookie);
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.GET != request.getMethod()) {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Only support GET method check if bookie is ready.");
            return response;
        }

        StateManager sm = bookie.getStateManager();
        if (sm.isRunning() && !sm.isShuttingDown()) {
            response.setCode(HttpServer.StatusCode.OK);
            response.setBody("");
        } else {
            response.setCode(HttpServer.StatusCode.SERVICE_UNAVAILABLE);
            response.setBody("Bookie is not fully started yet");
        }
        return response;
    }
}
