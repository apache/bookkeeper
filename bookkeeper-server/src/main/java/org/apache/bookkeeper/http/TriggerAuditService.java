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
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.service.HttpService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.replication.AuditorElector;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpService that handle Bookkeeper Configuration related http request.
 */
public class TriggerAuditService implements HttpService {

    static final Logger LOG = LoggerFactory.getLogger(TriggerAuditService.class);

    protected ServerConfiguration conf;

    public TriggerAuditService(ServerConfiguration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    /*
     * Force trigger the Audit by resetting the lostBookieRecoveryDelay.
     */
    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.PUT == request.getMethod() || HttpServer.Method.POST == request.getMethod()) {
            ClientConfiguration adminConf = new ClientConfiguration(conf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);

            try {
                admin.triggerAudit();
            } catch (Exception e) {
                e.printStackTrace();
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("Exception when do operation." + e.getMessage());
                return response;
            }

            response.setCode(HttpServer.StatusCode.OK);
            response.setBody("Success trigger audit.");
            LOG.debug("response body:" + response.getBody());
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be PUT/POST method");
            return response;
        }
    }
}
