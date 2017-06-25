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

import com.twitter.finagle.Service;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.util.Future;


public class TwitterHandlerFactory extends AbstractHandlerFactory<Service<Request, Response>> {
    public TwitterHandlerFactory(ServerOptions serverOptions) {
        super(serverOptions);
    }

    @Override
    public Service<Request, Response> newHeartbeatHandler() {
        return new Service<Request, Response>() {
            @Override
            public Future<Response> apply(Request request) {
                Response response = Response.apply();
                HeartbeatService service = getHeartbeatService();
                response.setContentString(service.getHeartbeat());
                return Future.value(response);
            }
        };
    }

    @Override
    public Service<Request, Response> newConfigurationHandler() {
        return new Service<Request, Response>() {
            @Override
            public Future<Response> apply(Request request) {
                Response response = Response.apply();
                ConfigService service = getConfigService();
                if (service == null) {
                    return handleServiceNotFound(response);
                }
                response.setContentString(service.getServerConfiguration());
                return Future.value(response);
            }
        };
    }

    @Override
    public Service<Request, Response> newBookieStatusHandler() {
        return new Service<Request, Response>() {
            @Override
            public Future<Response> apply(Request request) {
                Response response = Response.apply();
                BookieStatusService service = getBookieStatusService();
                if (request.method().toString().equals("GET") && service != null) {
                    response.setContentString(service.getBookieStatus());
                    return Future.value(response);
                } else {
                    return handleServiceNotFound(response);
                }
            }
        };
    }

    private Future<Response> handleServiceNotFound(Response response) {
        response.setStatusCode(StatusCode.NOT_FOUND.getValue());
        return Future.value(response);
    }
}
