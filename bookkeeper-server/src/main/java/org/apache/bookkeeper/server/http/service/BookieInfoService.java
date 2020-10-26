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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;

/**
 * HttpEndpointService that exposes the current info of the bookie.
 *
 * <pre>
 * <code>
 * {
 *  "freeSpace" : 0,
 *  "totalSpace" : 0
 * }
 * </code>
 * </pre>
 */
@AllArgsConstructor
public class BookieInfoService implements HttpEndpointService {
    @NonNull private final Bookie bookie;

    /**
     * POJO definition for the bookie info response.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BookieInfo {
        private long freeSpace;
        private long totalSpace;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.GET != request.getMethod()) {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Only GET is supported.");
            return response;
        }

        BookieInfo bi = new BookieInfo(bookie.getTotalFreeSpace(), bookie.getTotalDiskSpace());

        String jsonResponse = JsonUtil.toJson(bi);
        response.setBody(jsonResponse);
        response.setCode(HttpServer.StatusCode.OK);
        return response;
    }
}
