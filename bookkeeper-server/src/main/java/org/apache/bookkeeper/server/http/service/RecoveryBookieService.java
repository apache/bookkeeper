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
import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithRegistrationManager;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.concurrent.ExecutorService;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * HttpEndpointService that handle Bookkeeper recovery related http request.
 *
 * <p>The PUT method will recovery bookie with provided parameter.
 * The parameter of input body should be like this format:
 * {
 *   "bookie_src": [ "bookie_src1", "bookie_src2"... ],
 *   "delete_cookie": &lt;bool_value&gt;
 * }
 */
@CustomLog
public class RecoveryBookieService implements HttpEndpointService {


    protected ServerConfiguration conf;
    protected BookKeeperAdmin bka;
    protected ExecutorService executor;

    public RecoveryBookieService(ServerConfiguration conf, BookKeeperAdmin bka, ExecutorService executor) {
        checkNotNull(conf);
        this.conf = conf;
        this.bka = bka;
        this.executor = executor;
    }

    /*
     * Example body as this:
     * {
     *   "bookie_src": [ "bookie_src1", "bookie_src2"... ],
     *   "delete_cookie": <bool_value>
     * }
     */
    static class RecoveryRequestJsonBody {
        @JsonProperty("bookie_src")
        public List<String> bookieSrc;

        @JsonProperty("delete_cookie")
        public boolean deleteCookie;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        String requestBody = request.getBody();
        RecoveryRequestJsonBody requestJsonBody;

        if (requestBody == null) {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("No request body provide.");
            return response;
        }

        try {
            requestJsonBody = JsonUtil.fromJson(requestBody, RecoveryRequestJsonBody.class);
            log.debug().attr("bookieSrc", requestJsonBody.bookieSrc.get(0))
                    .attr("deleteCookie", requestJsonBody.deleteCookie)
                    .log("recovery request");
        } catch (JsonUtil.ParseJsonException e) {
            log.error().exception(e).log("Failed to parse JSON request");
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("ERROR parameters: " + e.getMessage());
            return response;
        }

        if (HttpServer.Method.PUT == request.getMethod() && !requestJsonBody.bookieSrc.isEmpty()) {
            runFunctionWithRegistrationManager(conf, rm -> {
                final String bookieSrcSerialized = requestJsonBody.bookieSrc.get(0);
                executor.execute(() -> {
                    try {
                        BookieId bookieSrc = BookieId.parse(bookieSrcSerialized);
                        boolean deleteCookie = requestJsonBody.deleteCookie;
                        log.info("Start recovering bookie");
                        bka.recoverBookieData(bookieSrc);
                        if (deleteCookie) {
                            Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(rm, bookieSrc);
                            cookie.getValue().deleteFromRegistrationManager(rm, bookieSrc, cookie.getVersion());
                        }
                        log.info("Complete recovering bookie");
                    } catch (Exception e) {
                        log.error().exception(e).log("Exception occurred while recovering bookie");
                    }
                });
                return null;
            });

            response.setCode(HttpServer.StatusCode.OK);
            response.setBody("Success send recovery request command.");
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be PUT method");
            return response;
        }
    }
}
