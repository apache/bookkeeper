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
import java.util.HashMap;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper Configuration related http request.
 */
public class DecommissionService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(DecommissionService.class);

    protected ServerConfiguration conf;

    public DecommissionService(ServerConfiguration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    /*
     * decommission bookie,
     */
    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.PUT == request.getMethod()) {
            ClientConfiguration adminConf = new ClientConfiguration(conf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
            String requestBody = request.getBody();

            if (requestBody == null) {
                admin.close();
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("Null request body for DecommissionService.");
                return response;
            }

            @SuppressWarnings("unchecked")
            HashMap<String, String> configMap = JsonUtil.fromJson(requestBody, HashMap.class);
            if (configMap != null && configMap.containsKey("bookie_src")) {
                try {
                    String bookieSrcString[] = configMap.get("bookie_src").split(":");
                    BookieSocketAddress bookieSrc = new BookieSocketAddress(
                      bookieSrcString[0], Integer.parseInt(bookieSrcString[1]));

                    admin.decommissionBookie(bookieSrc);
                    admin.close();
                    response.setCode(HttpServer.StatusCode.OK);
                    response.setBody("Success decommission Bookie " + bookieSrc.toString());
                    return response;
                } catch (Exception e) {
                    admin.close();
                    LOG.error("Meet Exception: ", e);
                    response.setCode(HttpServer.StatusCode.NOT_FOUND);
                    response.setBody("Exception when do decommission." + e.getMessage());
                    return response;
                }
            } else {
                admin.close();
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("Request body not contains bookie_src.");
                return response;
            }
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be PUT method");
            return response;
        }
    }
}
