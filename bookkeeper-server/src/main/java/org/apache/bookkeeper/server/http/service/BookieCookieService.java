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

import java.net.UnknownHostException;
import java.util.Map;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookieCookieService implements HttpEndpointService {
    static final Logger LOG = LoggerFactory.getLogger(BookieCookieService.class);
    private final ServerConfiguration conf;

    public BookieCookieService(ServerConfiguration conf) {
        this.conf = conf;
    }

    @SuppressWarnings("checkstyle:RegexpSingleline")
    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        Map<String, String> params = request.getParams();
        if (params == null || !params.containsKey("bookie_id")) {
            return new HttpServiceResponse("Not found bookie id. Should provide bookie_id=<ip:port>",
                    HttpServer.StatusCode.BAD_REQUEST);
        }
        if (request.getMethod() != HttpServer.Method.GET) {
            return new HttpServiceResponse("Not support for cookie update.", HttpServer.StatusCode.BAD_REQUEST);
        }

        String bookieIdStr = params.get("bookie_id");
        try {
            new BookieSocketAddress(bookieIdStr);
        } catch (UnknownHostException nhe) {
            return new HttpServiceResponse("Illegal bookie id. Should provide bookie_id=<ip:port>",
                    HttpServer.StatusCode.BAD_REQUEST);
        }

        BookieId bookieId = BookieId.parse(bookieIdStr);
        return MetadataDrivers.runFunctionWithRegistrationManager(conf, registrationManager -> {
            try {
                if (request.getMethod() == HttpServer.Method.GET) {
                    Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(registrationManager, bookieId);
                    return new HttpServiceResponse(cookie.getValue().toString(), HttpServer.StatusCode.OK);
                } else {
                    return new HttpServiceResponse("Method not allowed. Should be GET method",
                            HttpServer.StatusCode.METHOD_NOT_ALLOWED);
                }
            } catch (BookieException.CookieNotFoundException e) {
                return new HttpServiceResponse("Not found cookie: " + bookieId, HttpServer.StatusCode.NOT_FOUND);
            } catch (BookieException e) {
                LOG.error("Failed to get bookie cookie: ", e);
                return new HttpServiceResponse("Request failed, e:" + e.getMessage(),
                        HttpServer.StatusCode.INTERNAL_ERROR);
            }
        });
    }
}
