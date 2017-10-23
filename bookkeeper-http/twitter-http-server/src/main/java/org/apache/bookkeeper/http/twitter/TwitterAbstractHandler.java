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

import com.twitter.finagle.Service;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.util.Future;

import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.ErrorHttpService;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;

/**
 * Http handler for TwitterServer.
 */
public abstract class TwitterAbstractHandler extends Service<Request, Response> {

    /**
     * Process the request using the given httpEndpointService.
     */
    Future<Response> processRequest(HttpEndpointService httpEndpointService, Request request) {
        HttpServiceRequest httpServiceRequest = new HttpServiceRequest()
            .setMethod(convertMethod(request))
            .setParams(convertParams(request))
            .setBody(request.contentString());
        HttpServiceResponse httpServiceResponse = null;
        try {
            httpServiceResponse = httpEndpointService.handle(httpServiceRequest);
        } catch (Exception e) {
            httpServiceResponse = new ErrorHttpService().handle(httpServiceRequest);
        }
        Response response = Response.apply();
        response.setContentString(httpServiceResponse.getBody());
        response.statusCode(httpServiceResponse.getStatusCode());
        return Future.value(response);
    }

    /**
     * Convert http request parameters to Map.
     */
    @SuppressWarnings("unchecked")
    Map<String, String> convertParams(Request request) {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> entry : request.getParams()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    /**
     * Convert http request method to the method that
     * can be recognized by HttpServer.
     */
    HttpServer.Method convertMethod(Request request) {
        switch (request.method().name()) {
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
