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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.PortManager;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class TestHttpServer extends BookKeeperClusterTestCase {

    public TestHttpServer() {
        super(1);
    }

    /**
     * Test TwitterHttpServer
     */
    @Test(timeout = 60000)
    public void testTwitterHttpServer() throws Exception {
        baseConf.setHttpServer(TwitterHttpServer.class);
        doTestHttpServer();
    }

    /**
     * Test VertxHttpServer
     */
    @Test(timeout = 60000)
    public void testVertxHttpServer() throws Exception {
        baseConf.setHttpServer(VertxHttpServer.class);
        doTestHttpServer();
    }


    private void doTestHttpServer() throws Exception {
        int port = PortManager.nextFreePort();
        ServerOptions serverOptions = new ServerOptions()
            .setPort(port)
            .setBookieServer(bs.get(0))
            .setServerConf(baseConf);
        HttpServer server = ServerLoader.loadHttpServer(baseConf);
        // check reflection works
        assertNotNull(server);
        if(baseConf.getHttpServer() == TwitterHttpServer.class) {
            assertTrue(server instanceof TwitterHttpServer);
        } else {
            assertTrue(server instanceof VertxHttpServer);
        }

        // start http server
        server.initialize(serverOptions);
        server.startServer();
        assertTrue(server.isRunning());

        // test heartbeat api
        HttpResponse response = sendGet(getUrl(port, HttpServer.HEARTBEAT));
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.responseCode);
        assertEquals("OK", response.responseBody);

        // test config api
        response = sendGet(getUrl(port, HttpServer.SERVER_CONFIG));
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.responseCode);
        JSONObject jsonResponse = new JSONObject(response.responseBody);
        JSONObject innerJson = jsonResponse.getJSONObject("server_config");
        assertTrue(innerJson.length() > 0);

        // test bookie status api when bookie is writable
        response = sendGet(getUrl(port, HttpServer.BOOKIE_STATUS));
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.responseCode);
        jsonResponse = new JSONObject(response.responseBody);
        String bookieStatus = jsonResponse.getString("bookie_status");
        assertEquals("writable", bookieStatus);

        // test bookie status api when bookie is read only
        bs.get(0).getBookie().doTransitionToReadOnlyMode();
        response = sendGet(getUrl(port, HttpServer.BOOKIE_STATUS));
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.responseCode);
        jsonResponse = new JSONObject(response.responseBody);
        bookieStatus = jsonResponse.getString("bookie_status");
        assertEquals("readonly", bookieStatus);

        // stop http server
        server.stopServer();
        assertFalse(server.isRunning());
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
        } catch (IOException e) {
            // no-ops
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
