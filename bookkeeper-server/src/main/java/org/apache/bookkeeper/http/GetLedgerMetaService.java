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
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.service.HttpService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.util.JsonUtil;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpService that handle Bookkeeper Configuration related http request.
 */
public class GetLedgerMetaService implements HttpService {

    static final Logger LOG = LoggerFactory.getLogger(GetLedgerMetaService.class);

    protected ServerConfiguration conf;

    public GetLedgerMetaService(ServerConfiguration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        Map<String, String> params = request.getParams();

        if (HttpServer.Method.GET == request.getMethod() && (params != null) && params.containsKey("ledger_id")) {
            Long ledgerId = Long.parseLong(params.get("ledger_id"));

            ZooKeeper zk = ZooKeeperClient.newBuilder()
              .connectString(conf.getZkServers())
              .sessionTimeoutMs(conf.getZkTimeout())
              .build();
            LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(conf, zk);
            LedgerManager manager = mFactory.newLedgerManager();

            // output <ledgerId: ledgerMetadata>
            Map<String, String> output = Maps.newHashMap();
            ListLedgerService.ReadLedgerMetadataCallback cb =
              new ListLedgerService.ReadLedgerMetadataCallback(ledgerId);
            manager.readLedgerMetadata(ledgerId, cb);
            LedgerMetadata md = cb.get();
            output.put(ledgerId.toString(), new String(md.serialize(), UTF_8));

            if (manager != null) {
                manager.close();
                mFactory.uninitialize();
            }
            if (zk != null) {
                zk.close();
            }

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
