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

import java.util.List;
import org.apache.bookkeeper.bookie.GarbageCollectionStatus;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.proto.BookieServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle get garbage collection details service.
 *
 * <p>Get Garbage Collection status, the output would be like:
 *        [ {
 *           "forceCompacting" : false,
 *           "majorCompacting" : false,
 *           "minorCompacting" : false,
 *           "lastMajorCompactionTime" : 1544578144944,
 *           "lastMinorCompactionTime" : 1544578144944,
 *           "majorCompactionCounter" : 1,
 *           "minorCompactionCounter" : 0
 *         } ]
 */
public class GCDetailsService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(GCDetailsService.class);

    protected ServerConfiguration conf;
    protected BookieServer bookieServer;

    public GCDetailsService(ServerConfiguration conf, BookieServer bookieServer) {
        checkNotNull(conf);
        checkNotNull(bookieServer);
        this.conf = conf;
        this.bookieServer = bookieServer;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.GET == request.getMethod()) {
            List<GarbageCollectionStatus> details = bookieServer.getBookie()
                .getLedgerStorage().getGarbageCollectionStatus();

            String jsonResponse = JsonUtil.toJson(details);
            if (LOG.isDebugEnabled()) {
                LOG.debug("output body:" + jsonResponse);
            }
            response.setBody(jsonResponse);
            response.setCode(HttpServer.StatusCode.OK);
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Only support GET method to retrieve GC details."
                + " If you want to trigger gc, send a POST to gc endpoint.");
            return response;
        }
    }
}
