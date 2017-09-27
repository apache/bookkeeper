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
import java.util.List;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.util.JsonUtil;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper Configuration related http request.
 */
public class RecoveryBookieService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(RecoveryBookieService.class);

    protected ServerConfiguration conf;

    public RecoveryBookieService(ServerConfiguration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    /*
     * Example body as this:
     * {
     *   "bookie_src": [ "bookie_src1", "bookie_src2"... ],
     *   "bookie_dest": [ "bookie_dest1", "bookie_dest2"... ],
     *   "delete_cookie": <bool_value>
     * }
     */
    static class RecoveryRequestJsonBody {
        public List<String> bookie_src;
        public List<String> bookie_dest;
        public boolean delete_cookie;

        /*public RecoveryRequestJsonBody () {
            bookie_src = null;
            bookie_dest = null;
            delete_cookie = false;
        }*/
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        String requestBody = request.getBody();
        RecoveryRequestJsonBody requestJsonBody;

        if (requestBody == null) {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("No request body provide.");
            return response;
        }

        try {
            requestJsonBody = JsonUtil.fromJson(requestBody, RecoveryRequestJsonBody.class);
            LOG.debug("bookie_src: " + requestJsonBody.bookie_src.get(0)
                + " bookie_dest: "
                + ((requestJsonBody.bookie_dest == null) ? "null" : requestJsonBody.bookie_dest.get(0))
                + " delete_cookie: " + requestJsonBody.delete_cookie);
        } catch (JsonUtil.ParseJsonException e) {
            e.printStackTrace();
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("ERROR parameters" + e.getMessage());
            return response;
        }

        if ((HttpServer.Method.POST == request.getMethod() || HttpServer.Method.PUT == request.getMethod()) &&
            !requestJsonBody.bookie_src.isEmpty()) {
            ClientConfiguration adminConf = new ClientConfiguration(conf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);

            String bookieSrcString[] = requestJsonBody.bookie_src.get(0).split(":");
            BookieSocketAddress bookieSrc = new BookieSocketAddress(
              bookieSrcString[0], Integer.parseInt(bookieSrcString[1]));
            BookieSocketAddress bookieDest = null;
            if ((requestJsonBody.bookie_dest != null) && !requestJsonBody.bookie_dest.isEmpty()) {
                String bookieDestString[] = requestJsonBody.bookie_dest.get(0).split(":");
                bookieDest = new BookieSocketAddress(bookieDestString[0],
                  Integer.parseInt(bookieDestString[1]));
            }
            boolean deleteCookie = requestJsonBody.delete_cookie;
            try {
                admin.recoverBookieData(bookieSrc, bookieDest);
                if (deleteCookie) {
                    Versioned<Cookie> cookie = Cookie.readFromZooKeeper(admin.getZooKeeper(), adminConf, bookieSrc);
                    cookie.getValue().deleteFromZooKeeper(admin.getZooKeeper(), adminConf, bookieSrc, cookie.getVersion());
                }
            } catch (Exception e) {
                e.printStackTrace();
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("ERROR handling request: " + e.getMessage());
                return response;
            }
            response.setCode(HttpServer.StatusCode.OK);
            response.setBody("Handled recovery request");
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be POST/PUT method");
            return response;
        }
    }
}
