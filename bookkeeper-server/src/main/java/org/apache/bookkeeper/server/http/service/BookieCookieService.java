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

import static java.nio.charset.StandardCharsets.UTF_8;

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
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookieCookieService implements HttpEndpointService {
    static final Logger LOG = LoggerFactory.getLogger(BookieCookieService.class);
    private final ServerConfiguration conf;

    public BookieCookieService(ServerConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        Map<String, String> params = request.getParams();
        if (params == null || !params.containsKey("bookie_id")) {
            return new HttpServiceResponse("Not found bookie id. Should provide bookie_id=<ip:port>",
                    HttpServer.StatusCode.BAD_REQUEST);
        }
        if (request.getMethod() == HttpServer.Method.PUT && !params.containsKey("cookie")) {
            return new HttpServiceResponse("Not found cookie. "
                    + "Should provide cookie=<cookie string>", HttpServer.StatusCode.BAD_REQUEST);
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
                switch (request.getMethod()) {
                    case PUT:
                        Versioned<byte[]> newCookieWithVersion = new Versioned<>(params.get("cookie").getBytes(UTF_8),
                                Version.NEW);
                        registrationManager.writeCookie(bookieId, newCookieWithVersion);
                        return new HttpServiceResponse("Added cookie: " + bookieId, HttpServer.StatusCode.OK);
                    case GET:
                        Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(registrationManager, bookieId);
                        return new HttpServiceResponse(cookie.getValue().toString(), HttpServer.StatusCode.OK);
                    case DELETE:
                        registrationManager.removeCookie(bookieId, new LongVersion(-1));
                        return new HttpServiceResponse("Deleted cookie: " + bookieId, HttpServer.StatusCode.OK);
                    default:
                        return new HttpServiceResponse("Method not allowed. Should be GET or DELETE method",
                                HttpServer.StatusCode.METHOD_NOT_ALLOWED);
                }
            } catch (BookieException.CookieNotFoundException e) {
                return new HttpServiceResponse("Not found cookie: " + bookieId, HttpServer.StatusCode.NOT_FOUND);
            } catch (BookieException.CookieExistException e) {
                return new HttpServiceResponse("Cookie exist for bookieId: " + bookieId, HttpServer.StatusCode.OK);
            } catch (BookieException e) {
                LOG.error("Failed to op bookie cookie: ", e);
                return new HttpServiceResponse("Request failed, e:" + e.getMessage(),
                        HttpServer.StatusCode.INTERNAL_ERROR);
            }
        });
    }
}
