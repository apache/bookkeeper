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
import static java.nio.charset.StandardCharsets.US_ASCII;

import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper read ledger entry related http request.
 *
 * <p>The GET method will print all entry content of wanted entry.
 * User should set wanted "ledger_id", and can choose only print out wanted entry
 * by set parameter "start_entry_id", "end_entry_id" and "page".
 */
public class ReadLedgerEntryService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(ReadLedgerEntryService.class);

    protected ServerConfiguration conf;
    protected BookKeeperAdmin bka;

    public ReadLedgerEntryService(ServerConfiguration conf, BookKeeperAdmin bka) {
        checkNotNull(conf);
        this.conf = conf;
        this.bka = bka;
    }

    static final Long ENTRIES_PER_PAE = 1000L;

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        Map<String, String> params = request.getParams();

        if (HttpServer.Method.GET == request.getMethod() && (params != null) && params.containsKey("ledger_id")) {
            Long ledgerId = Long.parseLong(params.get("ledger_id"));
            Long startEntryId = 0L;
            Long endEntryId = -1L;
            if (params.containsKey("start_entry_id")) {
                startEntryId = Long.parseLong(params.get("start_entry_id"));
            }
            if (params.containsKey("end_entry_id")) {
                endEntryId = Long.parseLong(params.get("end_entry_id"));
            }

            // output <entryid: entry_content>
            Map<String, String> output = Maps.newHashMap();

            // Page index should start from 1;
            Integer pageIndex = params.containsKey("page") ? Integer.parseInt(params.get("page")) : -1;
            if (pageIndex > 0) {
                // start and end ledger index for wanted page.
                Long startIndexInPage = (pageIndex - 1) * ENTRIES_PER_PAE;
                Long endIndexInPage = startIndexInPage + ENTRIES_PER_PAE - 1;

                if ((startEntryId == 0L) || (startEntryId < startIndexInPage)) {
                    startEntryId = startIndexInPage;
                }
                if ((endEntryId == -1L) || (endEntryId > endIndexInPage)) {
                    endEntryId = endIndexInPage;
                }
                output.put("Entries for page: ", pageIndex.toString());
            }

            if (endEntryId != -1L && startEntryId > endEntryId) {
                response.setCode(HttpServer.StatusCode.INTERNAL_ERROR);
                response.setBody("parameter for start_entry_id: " + startEntryId
                    + " and end_entry_id: " + endEntryId + " conflict with page=" + pageIndex);
                return response;
            }

            Iterator<LedgerEntry> entries = bka.readEntries(ledgerId, startEntryId, endEntryId).iterator();
            while (entries.hasNext()) {
                LedgerEntry entry = entries.next();
                output.put(Long.valueOf(entry.getEntryId()).toString(), new String(entry.getEntry(), US_ASCII));
            }

            String jsonResponse = JsonUtil.toJson(output);
            LOG.debug("output body:" + jsonResponse);
            response.setBody(jsonResponse);
            response.setCode(HttpServer.StatusCode.OK);
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be GET method, with ledger_id provided");
            return response;
        }
    }
}
