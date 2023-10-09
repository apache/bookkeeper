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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.tools.cli.commands.bookie.SanityTestCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that exposes the bookie sanity state.
 *
 * <p>
 * Get the current bookie sanity response:
 *
 * <pre>
 * <code>
 * {
 *  "passed" : true,
 *  "readOnly" : false
 *}
 * </code>
 * </pre>
 */
public class BookieSanityService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(BookieSanityService.class);
    private final ServerConfiguration config;
    private Semaphore lock = new Semaphore(1);
    private static final int TIMEOUT_MS = 5000;
    private static final int MAX_CONCURRENT_REQUESTS = 1;

    public BookieSanityService(ServerConfiguration config) {
        this.config = checkNotNull(config);
    }

    /**
     * POJO definition for the bookie sanity response.
     */
    @Data
    @NoArgsConstructor
    public static class BookieSanity {
        private boolean passed;
        private boolean readOnly;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.GET != request.getMethod()) {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Only support GET method to retrieve bookie sanity state.");
            return response;
        }

        BookieSanity bs = new BookieSanity();
        if (config.isForceReadOnlyBookie()) {
            bs.readOnly = true;
        } else {
            try {
                // allow max concurrent request as sanity-test check relatively
                // longer time to complete
                try {
                    lock.tryAcquire(MAX_CONCURRENT_REQUESTS, TIMEOUT_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOG.error("Timing out due to max {} of sanity request are running concurrently",
                            MAX_CONCURRENT_REQUESTS);
                    response.setCode(HttpServer.StatusCode.INTERNAL_ERROR);
                    response.setBody("Timing out due to max number of sanity request are running concurrently");
                    return response;
                }
                SanityTestCommand sanity = new SanityTestCommand();
                bs.passed = sanity.apply(config, new SanityTestCommand.SanityFlags());
            } finally {
                lock.release();
            }
        }
        String jsonResponse = JsonUtil.toJson(bs);
        response.setBody(jsonResponse);
        response.setCode(HttpServer.StatusCode.OK);
        return response;
    }
}
