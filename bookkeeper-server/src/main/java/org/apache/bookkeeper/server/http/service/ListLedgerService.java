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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
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

    public ListLedgerService(ServerConfiguration conf, BookieServer bookieServer) {
        checkNotNull(conf);
        this.conf = conf;
        this.bookieServer = bookieServer;
    }

    // Number of LedgerMetadata contains in each page
    static final int LIST_LEDGER_BATCH_SIZE = 100;

    /**
     * Callback for reading ledger metadata.
     */
    public static class ReadLedgerMetadataCallback extends AbstractFuture<LedgerMetadata>
      implements BookkeeperInternalCallbacks.GenericCallback<Versioned<LedgerMetadata>> {
        final long ledgerId;

        ReadLedgerMetadataCallback(long ledgerId) {
            this.ledgerId = ledgerId;
        }

        long getLedgerId() {
            return ledgerId;
        }

        public void operationComplete(int rc, Versioned<LedgerMetadata> result) {
            if (rc != 0) {
                setException(BKException.create(rc));
            } else {
                set(result.getValue());
            }
        }
    }
    static void keepLedgerMetadata(ReadLedgerMetadataCallback cb, LinkedHashMap<String, String> output)
            throws Exception {
        LedgerMetadata md = cb.get();
        output.put(Long.valueOf(cb.getLedgerId()).toString(), new String(md.serialize(), UTF_8));
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

            // Page index should start from 1;
            int pageIndex = (printMeta && params.containsKey("page"))
                ? Integer.parseInt(params.get("page")) : -1;

            LedgerManagerFactory mFactory = bookieServer.getBookie().getLedgerManagerFactory();
            LedgerManager manager = mFactory.newLedgerManager();
            LedgerManager.LedgerRangeIterator iter = manager.getLedgerRanges();

            // output <ledgerId: ledgerMetadata>
            LinkedHashMap<String, String> output = Maps.newLinkedHashMap();
            // futures for readLedgerMetadata for each page.
            List<ReadLedgerMetadataCallback> futures = Lists.newArrayListWithExpectedSize(LIST_LEDGER_BATCH_SIZE);

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
                            ReadLedgerMetadataCallback cb = new ReadLedgerMetadataCallback(lid);
                            manager.readLedgerMetadata(lid, cb);
                            futures.add(cb);
                        }
                    }
                    if (futures.size() >= LIST_LEDGER_BATCH_SIZE) {
                        while (!futures.isEmpty()) {
                            ReadLedgerMetadataCallback cb = futures.remove(0);
                            keepLedgerMetadata(cb, output);
                        }
                    }
                }
                while (!futures.isEmpty()) {
                    ReadLedgerMetadataCallback cb = futures.remove(0);
                    keepLedgerMetadata(cb, output);
                }
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
