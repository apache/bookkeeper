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

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper list bookies related http request.
 * The GET method will list all bookies of type rw|ro in this bookkeeper cluster.
 */
public class ListBookiesService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(ListBookiesService.class);

    protected ServerConfiguration conf;
    protected BookKeeperAdmin bka;

    public ListBookiesService(ServerConfiguration conf, BookKeeperAdmin bka) {
        checkNotNull(conf);
        this.conf = conf;
        this.bka = bka;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        // GET
        if (HttpServer.Method.GET == request.getMethod()) {
            Collection<BookieSocketAddress> bookies = new ArrayList<BookieSocketAddress>();

            Map<String, String> params = request.getParams();
            // default print rw
            boolean readOnly = (params != null)
                && params.containsKey("type")
                && params.get("type").equals("ro");
            // default not print hostname
            boolean printHostname = (params != null)
                && params.containsKey("print_hostnames")
                && params.get("print_hostnames").equals("true");

            if (readOnly) {
                bookies.addAll(bka.getReadOnlyBookies());
            } else {
                bookies.addAll(bka.getAvailableBookies());
            }

            // output <bookieSocketAddress: hostname>
            Map<String, String> output = Maps.newHashMap();
            for (BookieSocketAddress b : bookies) {
                output.putIfAbsent(b.toString(), printHostname ? b.getHostName() : null);
                LOG.debug("bookie: " + b.toString() + " hostname:" + b.getHostName());
            }
            String jsonResponse = JsonUtil.toJson(output);

            response.setBody(jsonResponse);
            response.setCode(HttpServer.StatusCode.OK);
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be GET method");
            return response;
        }
    }
}
