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
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HttpEndpointService that handle force trigger entry location compact requests.
 *
 * <p>The PUT method will trigger entry location compact on current bookie.
 *
 * <p>The GET method will get the entry location compact running or not.
 * Output would be like:
 *        {
 *           "/data1/bookkeeper/ledgers/current/locations" : "false",
 *           "/data2/bookkeeper/ledgers/current/locations" : "true",
 *        }
 */

public class TriggerLocationCompactService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(TriggerLocationCompactService.class);

    private final BookieServer bookieServer;
    private final List<String> entryLocationDBPath;

    public TriggerLocationCompactService(BookieServer bookieServer) {
        this.bookieServer = checkNotNull(bookieServer);
        this.entryLocationDBPath = bookieServer.getBookie().getLedgerStorage().getEntryLocationDBPath();
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        LedgerStorage ledgerStorage = bookieServer.getBookie().getLedgerStorage();

        if (HttpServer.Method.PUT.equals(request.getMethod())) {
            String requestBody = request.getBody();
            String output = "Not trigger Entry Location RocksDB compact.";

            if (StringUtils.isBlank(requestBody)) {
                output = "Empty request body";
                response.setBody(output);
                response.setCode(HttpServer.StatusCode.BAD_REQUEST);
                return response;
            }

            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> configMap = JsonUtil.fromJson(requestBody, HashMap.class);
                Boolean isEntryLocationCompact = (Boolean) configMap
                        .getOrDefault("entryLocationRocksDBCompact", false);
                String entryLocations = (String) configMap.getOrDefault("entryLocations", "");

                if (!isEntryLocationCompact) {
                    // If entryLocationRocksDBCompact is false, doing nothing.
                    response.setBody(output);
                    response.setCode(HttpServer.StatusCode.OK);
                    return response;
                }
                if (StringUtils.isNotBlank(entryLocations)) {
                    // Specified trigger RocksDB compact entryLocations.
                    Set<String> locations = Sets.newHashSet(entryLocations.trim().split(","));
                    if (CollectionUtils.isSubCollection(locations, entryLocationDBPath)) {
                        ledgerStorage.entryLocationCompact(Lists.newArrayList(locations));
                        output = String.format("Triggered entry Location RocksDB: %s compact on bookie:%s.",
                                entryLocations, bookieServer.getBookieId());
                        response.setCode(HttpServer.StatusCode.OK);
                    } else {
                        output = String.format("Specified trigger compact entryLocations: %s is invalid. "
                                + "Bookie entry location RocksDB path: %s.", entryLocations, entryLocationDBPath);
                        response.setCode(HttpServer.StatusCode.BAD_REQUEST);
                    }
                } else {
                    // Not specified trigger compact entryLocations, trigger compact for all entry location.
                    ledgerStorage.entryLocationCompact();
                    output = "Triggered entry Location RocksDB compact on bookie:" + bookieServer.getBookieId();
                    response.setCode(HttpServer.StatusCode.OK);
                }
            } catch (JsonUtil.ParseJsonException ex) {
                output = ex.getMessage();
                response.setCode(HttpServer.StatusCode.BAD_REQUEST);
                LOG.warn("Trigger entry location index RocksDB compact failed, caused by: " + ex.getMessage());
            }

            String jsonResponse = JsonUtil.toJson(output);
            if (LOG.isDebugEnabled()) {
                LOG.debug("output body:" + jsonResponse);
            }
            response.setBody(jsonResponse);
            return response;
        } else if (HttpServer.Method.GET == request.getMethod()) {
            Map<String, Boolean> compactStatus = ledgerStorage.isEntryLocationCompacting(entryLocationDBPath);
            String jsonResponse = JsonUtil.toJson(compactStatus);
            if (LOG.isDebugEnabled()) {
                LOG.debug("output body:" + jsonResponse);
            }
            response.setBody(jsonResponse);
            response.setCode(HttpServer.StatusCode.OK);
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.METHOD_NOT_ALLOWED);
            response.setBody("Not found method. Should be PUT to trigger entry location compact,"
                    + " Or GET to get entry location compact state.");
            return response;
        }
    }
}