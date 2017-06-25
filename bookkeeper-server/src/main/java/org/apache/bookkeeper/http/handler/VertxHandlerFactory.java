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

package org.apache.bookkeeper.http.handler;

import org.apache.bookkeeper.http.HttpServer.StatusCode;
import org.apache.bookkeeper.http.ServerOptions;
import org.apache.bookkeeper.http.service.BookieStatusService;
import org.apache.bookkeeper.http.service.ConfigService;
import org.apache.bookkeeper.http.service.HeartbeatService;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

public class VertxHandlerFactory extends AbstractHandlerFactory<Handler<RoutingContext>> {


    public VertxHandlerFactory(ServerOptions serverOptions) {
        super(serverOptions);
    }

    @Override
    public Handler<RoutingContext> newHeartbeatHandler() {
        return context -> {
            HttpServerResponse response = context.response();
            HeartbeatService service = getHeartbeatService();
            response.end(service.getHeartbeat());
        };
    }

    @Override
    public Handler<RoutingContext> newConfigurationHandler() {
        return context -> {
            HttpServerResponse response = context.response();
            ConfigService service = getConfigService();
            if (service == null) {
                handleServiceNotFound(response);
                return;
            }
            String responseBody = service.getServerConfiguration();
            response.end(responseBody);
        };
    }

    @Override
    public Handler<RoutingContext> newBookieStatusHandler() {
        return context -> {
            HttpServerResponse response = context.response();
            HttpServerRequest request = context.request();
            BookieStatusService service = getBookieStatusService();
            if (request.method().equals(HttpMethod.GET) && service != null) {
                String responseBody = service.getBookieStatus();
                response.end(responseBody);
            } else {
                handleServiceNotFound(response);
            }
        };
    }


    private void handleServiceNotFound(HttpServerResponse response) {
        response.setStatusCode(StatusCode.NOT_FOUND.getValue());
        response.end();
    }


}
