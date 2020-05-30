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
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Maps;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper list ledger related http request.
 *
 * <p>The GET method will list all ledger_ids in this bookkeeper cluster.
 * User can choose print metadata of each ledger or not by set parameter "print_metadata"
 */
public class ListLedgerService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(ListLedgerService.class);

    protected ServerConfiguration conf;
    protected BookieServer bookieServer;
    private final LedgerMetadataSerDe serDe;

    public ListLedgerService(ServerConfiguration conf, BookieServer bookieServer) {
        checkNotNull(conf);
        this.conf = conf;
        this.bookieServer = bookieServer;
        this.serDe = new LedgerMetadataSerDe();

    }

    // Number of LedgerMetadata contains in each page
    static final int LIST_LEDGER_BATCH_SIZE = 100;

    private void keepLedgerMetadata(long ledgerId, CompletableFuture<Versioned<LedgerMetadata>> future,
                                    LinkedHashMap<String, Object> output, boolean decodeMeta)
            throws Exception {
        LedgerMetadata md = future.get().getValue();
        if (decodeMeta) {
            output.put(Long.valueOf(ledgerId).toString(), md);
        } else {
            output.put(Long.valueOf(ledgerId).toString(), new String(serDe.serialize(md), UTF_8));
        }
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        // GET
        // parameter could be like: print_metadata=true&page=PageIndex
        if (HttpServer.Method.GET == request.getMethod()) {
            Map<String, String> params = request.getParams();
            // default not print metadata
            boolean printMeta = (params != null)
              && params.containsKey("print_metadata")
              && params.get("print_metadata").equals("true");

            // do not decode meta by default for backward compatibility
            boolean decodeMeta = (params != null)
                    && params.getOrDefault("decode_meta", "false").equals("true");

            // Page index should start from 1;
            int pageIndex = (printMeta && params.containsKey("page"))
                ? Integer.parseInt(params.get("page")) : -1;

            LedgerManagerFactory mFactory = bookieServer.getBookie().getLedgerManagerFactory();
            LedgerManager manager = mFactory.newLedgerManager();
            LedgerManager.LedgerRangeIterator iter = manager.getLedgerRanges(0);

            // output <ledgerId: ledgerMetadata>
            LinkedHashMap<String, Object> output = Maps.newLinkedHashMap();
            // futures for readLedgerMetadata for each page.
            Map<Long, CompletableFuture<Versioned<LedgerMetadata>>> futures =
                new LinkedHashMap<>(LIST_LEDGER_BATCH_SIZE);

            if (printMeta) {
                int ledgerIndex = 0;

                // start and end ledger index for wanted page.
                int startLedgerIndex = 0;
                int endLedgerIndex = 0;
                if (pageIndex > 0) {
                    startLedgerIndex = (pageIndex - 1) * LIST_LEDGER_BATCH_SIZE;
                    endLedgerIndex = startLedgerIndex + LIST_LEDGER_BATCH_SIZE - 1;
                }

                // get metadata
                while (iter.hasNext()) {
                    LedgerManager.LedgerRange r = iter.next();
                    for (Long lid : r.getLedgers()) {
                        ledgerIndex++;
                        if (endLedgerIndex == 0       // no actual page parameter provided
                                || (ledgerIndex >= startLedgerIndex && ledgerIndex <= endLedgerIndex)) {
                            futures.put(lid, manager.readLedgerMetadata(lid));
                        }
                    }
                    if (futures.size() >= LIST_LEDGER_BATCH_SIZE) {
                        for (Map.Entry<Long, CompletableFuture<Versioned<LedgerMetadata>> > e : futures.entrySet()) {
                            keepLedgerMetadata(e.getKey(), e.getValue(), output, decodeMeta);
                        }
                        futures.clear();
                    }
                }
                for (Map.Entry<Long, CompletableFuture<Versioned<LedgerMetadata>> > e : futures.entrySet()) {
                    keepLedgerMetadata(e.getKey(), e.getValue(), output, decodeMeta);
                }
                futures.clear();
            } else {
                while (iter.hasNext()) {
                    LedgerManager.LedgerRange r = iter.next();
                    for (Long lid : r.getLedgers()) {
                        output.put(lid.toString(), null);
                    }
                }
            }

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
