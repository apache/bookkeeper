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

import lombok.AllArgsConstructor;
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
 * HttpEndpointService that handles readOnly state related http requests.
 * The GET method will get the current readOnly state of the bookie.
 * The PUT method will change the current readOnly state of the bookie if the desired state is
 * different from the current. The request body could be {"readOnly":true/false}. The current
 * or the updated state will be included in the response.
 */
public class BookieStateReadOnlyService implements HttpEndpointService {
    private final Bookie bookie;

    public BookieStateReadOnlyService(Bookie bookie) {
        this.bookie = checkNotNull(bookie);
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        StateManager stateManager = this.bookie.getStateManager();

        if (HttpServer.Method.PUT.equals(request.getMethod())) {
            ReadOnlyState inState = JsonUtil.fromJson(request.getBody(), ReadOnlyState.class);
            if (stateManager.isReadOnly() && !inState.isReadOnly()) {
                if (stateManager.isForceReadOnly()) {
                    response.setCode(HttpServer.StatusCode.BAD_REQUEST);
                    response.setBody("Bookie is in forceReadOnly mode, cannot transit to writable mode");
                    return response;
                }
                stateManager.transitionToWritableMode().get();
            } else if (!stateManager.isReadOnly() && inState.isReadOnly()) {
                stateManager.transitionToReadOnlyMode().get();
            }
        } else if (!HttpServer.Method.GET.equals(request.getMethod())) {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Unsupported method. Should be GET or PUT method");
            return response;
        }

        ReadOnlyState outState = new ReadOnlyState(stateManager.isReadOnly());
        response.setBody(JsonUtil.toJson(outState));
        response.setCode(HttpServer.StatusCode.OK);

        return response;
    }

    /**
     * The object represent the readOnly state.
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class ReadOnlyState {
        private boolean readOnly;
    }
}
