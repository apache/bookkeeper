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

import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper list under replicated ledger related http request.
 *
 * <p>The GET method will list all ledger_ids of under replicated ledger.
 * User can filer wanted ledger by set parameter "missingreplica" and "excludingmissingreplica"
 */
public class ListUnderReplicatedLedgerService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(ListUnderReplicatedLedgerService.class);

    protected ServerConfiguration conf;
    protected BookieServer bookieServer;

    public ListUnderReplicatedLedgerService(ServerConfiguration conf, BookieServer bookieServer) {
        checkNotNull(conf);
        this.conf = conf;
        this.bookieServer = bookieServer;
    }

    /*
     * Print the node which holds the auditor lock.
     */
    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        // parameter as this: ?missingreplica=<bookie_address>&excludingmissingreplica=<bookid_address>
        Map<String, String> params = request.getParams();

        if (HttpServer.Method.GET == request.getMethod()) {
            final String includingBookieId;
            final String excludingBookieId;
            boolean printMissingReplica = false;

            if (params != null && params.containsKey("missingreplica")) {
                includingBookieId = params.get("missingreplica");
            } else {
                includingBookieId = null;
            }
            if (params != null && params.containsKey("excludingmissingreplica")) {
                excludingBookieId = params.get("excludingmissingreplica");
            } else {
                excludingBookieId = null;
            }
            if (params != null && params.containsKey("printmissingreplica")) {
                printMissingReplica = true;
            }
            Predicate<List<String>> predicate = null;
            if (!StringUtils.isBlank(includingBookieId) && !StringUtils.isBlank(excludingBookieId)) {
                predicate = replicasList -> (replicasList.contains(includingBookieId)
                  && !replicasList.contains(excludingBookieId));
            } else if (!StringUtils.isBlank(includingBookieId)) {
                predicate = replicasList -> replicasList.contains(includingBookieId);
            } else if (!StringUtils.isBlank(excludingBookieId)) {
                predicate = replicasList -> !replicasList.contains(excludingBookieId);
            }

            try {
                boolean hasURLedgers = false;
                List<Long> outputLedgers = null;
                Map<Long, List<String>> outputLedgersWithMissingReplica = null;
                LedgerManagerFactory mFactory = bookieServer.getBookie().getLedgerManagerFactory();
                LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                Iterator<UnderreplicatedLedger> iter = underreplicationManager.listLedgersToRereplicate(predicate);

                hasURLedgers = iter.hasNext();
                if (hasURLedgers) {
                    if (printMissingReplica) {
                        outputLedgersWithMissingReplica = new LinkedHashMap<Long, List<String>>();
                    } else {
                        outputLedgers = Lists.newArrayList();
                    }
                }
                while (iter.hasNext()) {
                    if (printMissingReplica) {
                        UnderreplicatedLedger underreplicatedLedger = iter.next();
                        outputLedgersWithMissingReplica.put(underreplicatedLedger.getLedgerId(),
                                underreplicatedLedger.getReplicaList());
                    } else {
                        outputLedgers.add(iter.next().getLedgerId());
                    }
                }
                if (!hasURLedgers) {
                    response.setCode(HttpServer.StatusCode.NOT_FOUND);
                    response.setBody("No under replicated ledgers found");
                    return response;
                } else {
                    response.setCode(HttpServer.StatusCode.OK);
                    String jsonResponse = JsonUtil
                            .toJson(printMissingReplica ? outputLedgersWithMissingReplica : outputLedgers);
                    LOG.debug("output body: " + jsonResponse);
                    response.setBody(jsonResponse);
                    return response;
                }
            } catch (Exception e) {
                LOG.error("Exception occurred while listing under replicated ledgers", e);
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("Exception when get." + e.getMessage());
                return response;
            }
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be GET method");
            return response;
        }
    }
}
