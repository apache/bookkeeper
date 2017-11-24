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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.JsonUtil;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper recovery related http request.
 *
 * The PUT method will recovery bookie with provided parameter.
 * The parameter of input body should be like this format:
 * {
 *   "bookie_src": [ "bookie_src1", "bookie_src2"... ],
 *   "bookie_dest": [ "bookie_dest1", "bookie_dest2"... ],
 *   "delete_cookie": <bool_value>
 * }
 */
public class RecoveryBookieService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(RecoveryBookieService.class);

    protected ServerConfiguration conf;
    protected BookKeeperAdmin bka;
    protected ExecutorService executor;

    public RecoveryBookieService(ServerConfiguration conf, BookKeeperAdmin bka, ExecutorService executor) {
        Preconditions.checkNotNull(conf);
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
        public List<String> bookie_src;
        public boolean delete_cookie;
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
            LOG.debug("bookie_src: [" + requestJsonBody.bookie_src.get(0)
                + "],  delete_cookie: [" + requestJsonBody.delete_cookie + "]");
        } catch (JsonUtil.ParseJsonException e) {
            LOG.error("Meet Exception: ", e);
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("ERROR parameters: " + e.getMessage());
            return response;
        }

        if (HttpServer.Method.PUT == request.getMethod() &&
            !requestJsonBody.bookie_src.isEmpty()) {

            Class<? extends RegistrationManager> rmClass = conf.getRegistrationManagerClass();
            RegistrationManager rm = ReflectionUtils.newInstance(rmClass);
            rm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);

            String bookieSrcString[] = requestJsonBody.bookie_src.get(0).split(":");
            BookieSocketAddress bookieSrc = new BookieSocketAddress(
              bookieSrcString[0], Integer.parseInt(bookieSrcString[1]));
            boolean deleteCookie = requestJsonBody.delete_cookie;
            executor.execute(() -> {
                try {
                    LOG.info("Start recovering bookie.");
                    bka.recoverBookieData(bookieSrc);
                    if (deleteCookie) {
                        Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(rm, bookieSrc);
                        cookie.getValue().deleteFromRegistrationManager(rm, bookieSrc, cookie.getVersion());
                    }
                    LOG.info("Complete recovering bookie");
                } catch (Exception e) {
                    LOG.error("Exception occurred while recovering bookie", e);
                }
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
