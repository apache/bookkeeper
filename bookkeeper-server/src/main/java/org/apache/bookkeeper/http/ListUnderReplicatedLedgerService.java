/**
 *
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
 *
 */
package org.apache.bookkeeper.http;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.service.HttpService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.replication.AuditorElector;
import org.apache.bookkeeper.util.JsonUtil;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpService that handle Bookkeeper Configuration related http request.
 */
public class ListUnderReplicatedLedgerService implements HttpService {

    static final Logger LOG = LoggerFactory.getLogger(ListUnderReplicatedLedgerService.class);

    protected ServerConfiguration conf;

    public ListUnderReplicatedLedgerService(ServerConfiguration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
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
            Predicate<List<String>> predicate = null;
            if (!StringUtils.isBlank(includingBookieId) && !StringUtils.isBlank(excludingBookieId)) {
                predicate = replicasList -> (replicasList.contains(includingBookieId)
                  && !replicasList.contains(excludingBookieId));
            } else if (!StringUtils.isBlank(includingBookieId)) {
                predicate = replicasList -> replicasList.contains(includingBookieId);
            } else if (!StringUtils.isBlank(excludingBookieId)) {
                predicate = replicasList -> !replicasList.contains(excludingBookieId);
            }

            ZooKeeper zk = ZooKeeperClient.newBuilder()
                .connectString(conf.getZkServers())
                .sessionTimeoutMs(conf.getZkTimeout())
                .build();

            try {
                List<Long> outputLedgers = Lists.newArrayList();
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(conf, zk);
                LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                Iterator<Long> iter = underreplicationManager.listLedgersToRereplicate(predicate);

                while (iter.hasNext()) {
                    outputLedgers.add(iter.next());
                }
                zk.close();
                if (outputLedgers.isEmpty()) {
                    response.setCode(HttpServer.StatusCode.NOT_FOUND);
                    response.setBody("No under replicated ledgers found");
                    return response;
                } else {
                    response.setCode(HttpServer.StatusCode.OK);
                    String jsonResponse = JsonUtil.toJson(outputLedgers);
                    LOG.debug("output body: " + jsonResponse);
                    response.setBody(jsonResponse);
                    return response;
                }
            } catch (Exception e) {
                e.printStackTrace();
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
