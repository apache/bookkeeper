/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.server.http.service;

import java.io.IOException;
import java.io.StringWriter;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer.Method;
import org.apache.bookkeeper.http.HttpServer.StatusCode;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.stats.StatsProvider;

/**
 * HttpEndpointService that handle exposing metrics.
 *
 * <p>The GET method will return all the emtrics collected at stats provider.
 */
public class MetricsService implements HttpEndpointService {

    private final ServerConfiguration conf;
    private final StatsProvider statsProvider;

    public MetricsService(ServerConfiguration conf,
                          StatsProvider statsProvider) {
        this.conf = conf;
        this.statsProvider = statsProvider;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        if (Method.GET != request.getMethod()) {
            response.setCode(StatusCode.FORBIDDEN);
            response.setBody(request.getMethod() + " is forbidden. Should be GET method");
            return response;
        }

        if (null == statsProvider) {
            response.setCode(StatusCode.INTERNAL_ERROR);
            response.setBody("Stats provider is not enabled. Please enable it by set statsProviderClass"
                + " on bookie configuration");
            return response;
        }

        // GET
        try (StringWriter writer = new StringWriter(1024)) {
            statsProvider.writeAllMetrics(writer);
            writer.flush();
            response.setCode(StatusCode.OK);
            response.setBody(writer.getBuffer().toString());
        } catch (UnsupportedOperationException uoe) {
            response.setCode(StatusCode.INTERNAL_ERROR);
            response.setBody("Currently stats provider doesn't support exporting metrics in http service");
        } catch (IOException ioe) {
            response.setCode(StatusCode.INTERNAL_ERROR);
            response.setBody("Exceptions are thrown when exporting metrics : " + ioe.getMessage());
        }
        return response;
    }
}
