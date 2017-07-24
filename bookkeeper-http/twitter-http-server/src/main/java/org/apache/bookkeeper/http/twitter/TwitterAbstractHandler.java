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

package org.apache.bookkeeper.http.twitter;

import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.ServiceRequest;
import org.apache.bookkeeper.http.service.ServiceResponse;

import com.twitter.finagle.Service;
import com.twitter.finagle.http.Method;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.util.Future;

public abstract class TwitterAbstractHandler extends Service<Request, Response> {

    Future<Response> processRequest(org.apache.bookkeeper.http.service.Service service, Request request) {
        ServiceRequest serviceRequest = new ServiceRequest()
            .setMethod(convertMethod(request.method()))
            .setParams(convertParams(request))
            .setBody(request.contentString());
        ServiceResponse serviceResponse = service.handle(serviceRequest);
        Response response = Response.apply();
        response.setContentString(serviceResponse.getBody());
        response.statusCode(serviceResponse.getStatusCode());
        return Future.value(response);
    }

    @SuppressWarnings("unchecked")
    Map convertParams(Request request) {
        Map map = new HashMap();
        for (Map.Entry<String, String> entry : request.getParams()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    HttpServer.Method convertMethod(Method method) {
        switch (method.name()) {
            case "GET":
                return HttpServer.Method.GET;
            case "POST":
                return HttpServer.Method.POST;
            case "DELETE":
                return HttpServer.Method.DELETE;
            case "PUT":
                return HttpServer.Method.PUT;
            default:
                return HttpServer.Method.GET;
        }
    }
}
