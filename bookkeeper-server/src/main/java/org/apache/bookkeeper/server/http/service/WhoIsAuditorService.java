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

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.replication.AuditorElector;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper who is auditor related http request.
 *
 * <p>The GET method will get the auditor bookie address
 */
public class WhoIsAuditorService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(WhoIsAuditorService.class);

    protected ServerConfiguration conf;
    protected ZooKeeper zk;

    public WhoIsAuditorService(ServerConfiguration conf, ZooKeeper zk) {
        checkNotNull(conf);
        this.conf = conf;
        this.zk = zk;
    }

    /*
     * Print the node which holds the auditor lock.
     */
    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.GET == request.getMethod()) {
            BookieSocketAddress bookieId = null;
            try {
                bookieId = AuditorElector.getCurrentAuditor(conf, zk);

                if (bookieId == null) {
                    response.setCode(HttpServer.StatusCode.NOT_FOUND);
                    response.setBody("No auditor elected");
                    return response;
                }
            } catch (Exception e) {
                LOG.error("Meet Exception: ", e);
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("Exception when get." + e.getMessage());
                return response;
            }

            response.setCode(HttpServer.StatusCode.OK);
            response.setBody("Auditor: "
                + bookieId.getSocketAddress().getAddress().getCanonicalHostName() + "/"
                + bookieId.getSocketAddress().getAddress().getHostAddress() + ":"
                + bookieId.getSocketAddress().getPort());
            LOG.debug("response body:" + response.getBody());
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be GET method");
            return response;
        }
    }
}
