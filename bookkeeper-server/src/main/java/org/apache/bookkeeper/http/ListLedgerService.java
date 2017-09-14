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
import org.apache.bookkeeper.client.LedgerMetadata;
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
public class ListLedgerService implements HttpService {

    static final Logger LOG = LoggerFactory.getLogger(ListLedgerService.class);

    protected ServerConfiguration conf;

    public ListLedgerService(ServerConfiguration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    static final int LIST_LEDGER_BATCH_SIZE = 1000;

    static class ReadLedgerMetadataCallback extends AbstractFuture<LedgerMetadata>
      implements BookkeeperInternalCallbacks.GenericCallback<LedgerMetadata> {
        final long ledgerId;

        ReadLedgerMetadataCallback(long ledgerId) {
            this.ledgerId = ledgerId;
        }

        long getLedgerId() {
            return ledgerId;
        }

        public void operationComplete(int rc, LedgerMetadata result) {
            if (rc != 0) {
                setException(BKException.create(rc));
            } else {
                set(result);
            }
        }
    }
    static void keepLedgerMetadata(ReadLedgerMetadataCallback cb, Map<String, String> output) throws Exception {
        LedgerMetadata md = cb.get();
        output.put(Long.valueOf(cb.getLedgerId()).toString(), new String(md.serialize(), UTF_8));
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        // GET
        if (HttpServer.Method.GET == request.getMethod()) {
            Collection<BookieSocketAddress> bookies = new ArrayList<BookieSocketAddress>();

            Map<String, String> params = request.getParams();
            // default not print metadata
            boolean printMeta = (params != null) &&
              params.containsKey("print_metadata") &&
              params.get("print_metadata").equals("true");

            ZooKeeper zk = ZooKeeperClient.newBuilder()
              .connectString(conf.getZkServers())
              .sessionTimeoutMs(conf.getZkTimeout())
              .build();
            LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(conf, zk);
            LedgerManager manager = mFactory.newLedgerManager();
            LedgerManager.LedgerRangeIterator iter = manager.getLedgerRanges();

            // output <ledgerId: ledgerMetadata>
            Map<String, String> output = Maps.newHashMap();
            // futures for readLedgerMetadata
            List<ReadLedgerMetadataCallback> futures = Lists.newArrayListWithExpectedSize(LIST_LEDGER_BATCH_SIZE);

            if (printMeta) {
                // get metadata
                while (iter.hasNext()) {
                    LedgerManager.LedgerRange r = iter.next();
                    for (Long lid : r.getLedgers()) {
                        ReadLedgerMetadataCallback cb = new ReadLedgerMetadataCallback(lid);
                        manager.readLedgerMetadata(lid, cb);
                        futures.add(cb);
                    }
                    if (futures.size() >= LIST_LEDGER_BATCH_SIZE) {
                        while (futures.size() > 0) {
                            ReadLedgerMetadataCallback cb = futures.remove(0);
                            keepLedgerMetadata(cb, output);
                        }
                    }
                }
                while (futures.size() > 0) {
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
