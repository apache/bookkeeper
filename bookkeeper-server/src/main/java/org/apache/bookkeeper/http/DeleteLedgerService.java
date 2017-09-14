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

import static com.google.common.base.Charsets.UTF_8;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.service.HttpService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.util.JsonUtil;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpService that handle Bookkeeper Configuration related http request.
 */
public class DeleteLedgerService implements HttpService {

    static final Logger LOG = LoggerFactory.getLogger(DeleteLedgerService.class);

    protected ServerConfiguration conf;

    public DeleteLedgerService(ServerConfiguration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        // only handle DELETE method
        if (HttpServer.Method.DELETE == request.getMethod()) {
            Map<String, String> params = request.getParams();
            if (params != null && params.containsKey("ledger_id")) {
                ClientConfiguration clientConf = new ClientConfiguration();
                clientConf.addConfiguration(conf);
                BookKeeper bk = new BookKeeper(clientConf);
                Long ledgerId = Long.parseLong(params.get("ledger_id"));

                bk.deleteLedger(ledgerId);

                String output = "Deleted ledger: " + ledgerId;
                String jsonResponse = JsonUtil.toJson(output);
                LOG.debug("output body:" + jsonResponse);
                response.setBody(jsonResponse);
                response.setCode(HttpServer.StatusCode.OK);
                return response;
            } else {
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("Not ledger found. Should provide ledger_id=<id>");
                return response;
            }
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be DELETE method");
            return response;
        }
    }
}
