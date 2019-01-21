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
import java.util.Map;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.proto.BookieServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper get ledger metadata related http request.
 * The GET method will get the ledger metadata for given "ledger_id".
 */
public class GetLedgerMetaService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(GetLedgerMetaService.class);

    protected ServerConfiguration conf;
    protected BookieServer bookieServer;
    private final LedgerMetadataSerDe serDe;

    public GetLedgerMetaService(ServerConfiguration conf, BookieServer bookieServer) {
        checkNotNull(conf);
        this.conf = conf;
        this.bookieServer = bookieServer;
        this.serDe = new LedgerMetadataSerDe();
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        Map<String, String> params = request.getParams();

        if (HttpServer.Method.GET == request.getMethod() && (params != null) && params.containsKey("ledger_id")) {
            Long ledgerId = Long.parseLong(params.get("ledger_id"));

            LedgerManagerFactory mFactory = bookieServer.getBookie().getLedgerManagerFactory();
            LedgerManager manager = mFactory.newLedgerManager();

            // output <ledgerId: ledgerMetadata>
            Map<String, Object> output = Maps.newHashMap();
            LedgerMetadata md = manager.readLedgerMetadata(ledgerId).get().getValue();
            output.put(ledgerId.toString(), md);

            manager.close();

            String jsonResponse = JsonUtil.toJson(output);
            LOG.debug("output body:" + jsonResponse);
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
