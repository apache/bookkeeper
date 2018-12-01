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
package org.apache.bookkeeper.http.vertx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.bookkeeper.http.HttpRouter;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServiceProvider;
import org.apache.bookkeeper.http.NullHttpServiceProvider;
import org.apache.bookkeeper.http.service.HeartbeatService;
import org.junit.Test;

/**
 * Unit test {@link VertxHttpServer}.
 */
public class TestVertxHttpServer {
    @Test
    public void testStartBasicHttpServer() throws Exception {
        VertxHttpServer httpServer = new VertxHttpServer();
        HttpServiceProvider httpServiceProvider = NullHttpServiceProvider.getInstance();
        httpServer.initialize(httpServiceProvider);
        assertTrue(httpServer.startServer(0));
        int port = httpServer.getListeningPort();
        HttpResponse httpResponse = sendGet(getUrl(port, HttpRouter.HEARTBEAT));
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpResponse.responseCode);
        assertEquals(HeartbeatService.HEARTBEAT.trim(), httpResponse.responseBody.trim());
        httpServer.stopServer();
    }

    @Test
    public void testStartMetricsServiceOnRouterPath() throws Exception {
        VertxHttpServer httpServer = new VertxHttpServer();
        HttpServiceProvider httpServiceProvider = NullHttpServiceProvider.getInstance();
        httpServer.initialize(httpServiceProvider);
        assertTrue(httpServer.startServer(0));
        int port = httpServer.getListeningPort();
        HttpResponse httpResponse = sendGet(getUrl(port, HttpRouter.METRICS));
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpResponse.responseCode);
        httpServer.stopServer();
    }

    // HTTP GET request
    private HttpResponse sendGet(String url) throws IOException {
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        // optional, default is GET
        con.setRequestMethod("GET");
        int responseCode = con.getResponseCode();
        StringBuilder response = new StringBuilder();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
        } finally {
            if (in != null) {
                in.close();
            }
        }
        return new HttpResponse(responseCode, response.toString());
    }

    private String getUrl(int port, String path) {
        return "http://localhost:" + port + path;
    }

    private class HttpResponse {
        private int responseCode;
        private String responseBody;

        public HttpResponse(int responseCode, String responseBody) {
            this.responseCode = responseCode;
            this.responseBody = responseBody;
        }
    }
}
