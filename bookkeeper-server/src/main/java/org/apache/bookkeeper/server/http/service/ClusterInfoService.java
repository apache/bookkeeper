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

import java.util.Iterator;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.net.BookieId;

/**
 * HttpEndpointService that exposes the current info about the cluster of bookies.
 *
 * <pre>
 * <code>
 * {
 *  "hasAuditorElected" : true,
 *  "auditorId" : "blah",
 *  "hasUnderReplicatedLedgers": false,
 *  "isLedgerReplicationEnabled": true,
 *  "totalBookiesCount": 10,
 *  "writableBookiesCount": 6,
 *  "readonlyBookiesCount": 3,
 *  "unavailableBookiesCount": 1
 * }
 * </code>
 * </pre>
 */
@AllArgsConstructor
@Slf4j
public class ClusterInfoService implements HttpEndpointService {

    @NonNull
    private final BookKeeperAdmin bka;
    @NonNull
    private final LedgerManagerFactory ledgerManagerFactory;

    /**
     * POJO definition for the cluster info response.
     */
    @Data
    public static class ClusterInfo {
        private boolean auditorElected;
        private String auditorId;
        private boolean clusterUnderReplicated;
        private boolean ledgerReplicationEnabled;
        private int totalBookiesCount;
        private int writableBookiesCount;
        private int readonlyBookiesCount;
        private int unavailableBookiesCount;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        final HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.GET != request.getMethod()) {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Only GET is supported.");
            return response;
        }

        final ClusterInfo info = new ClusterInfo();
        fillUReplicatedInfo(info);
        fillAuditorInfo(info);
        fillBookiesInfo(info);

        String jsonResponse = JsonUtil.toJson(info);
        response.setBody(jsonResponse);
        response.setCode(HttpServer.StatusCode.OK);
        return response;
    }

    @SneakyThrows
    private void fillBookiesInfo(ClusterInfo info) {
        int totalBookiesCount = bka.getAllBookies().size();
        int writableBookiesCount = bka.getAvailableBookies().size();
        int readonlyBookiesCount = bka.getReadOnlyBookies().size();
        int unavailableBookiesCount = totalBookiesCount - writableBookiesCount - readonlyBookiesCount;

        info.setTotalBookiesCount(totalBookiesCount);
        info.setWritableBookiesCount(writableBookiesCount);
        info.setReadonlyBookiesCount(readonlyBookiesCount);
        info.setUnavailableBookiesCount(unavailableBookiesCount);
    }

    private void fillAuditorInfo(ClusterInfo info) {
        try {
            BookieId currentAuditor = bka.getCurrentAuditor();
            info.setAuditorElected(currentAuditor != null);
            info.setAuditorId(currentAuditor == null ? "" : currentAuditor.getId());
        } catch (Exception e) {
            log.error("Could not get Auditor info", e);
            info.setAuditorElected(false);
            info.setAuditorId("");
        }
    }

    @SneakyThrows
    private void fillUReplicatedInfo(ClusterInfo info) {
        try (LedgerUnderreplicationManager underreplicationManager =
                ledgerManagerFactory.newLedgerUnderreplicationManager()) {
            Iterator<UnderreplicatedLedger> iter = underreplicationManager.listLedgersToRereplicate(null);

            info.setClusterUnderReplicated(iter.hasNext());
            info.setLedgerReplicationEnabled(underreplicationManager.isLedgerReplicationEnabled());
        }
    }

}
