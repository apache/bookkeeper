/*
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.io.Files;
import io.vertx.ext.web.handler.BodyHandler;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
        HttpResponse httpResponse = send(getUrl(port, HttpRouter.HEARTBEAT), HttpServer.Method.GET);
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpResponse.responseCode);
        assertEquals(HeartbeatService.HEARTBEAT.trim(), httpResponse.responseBody.trim());
        httpServer.stopServer();
    }

    @Test
    public void testStartBasicHttpServerConfigHost() throws Exception {
        VertxHttpServer httpServer = new VertxHttpServer();
        HttpServiceProvider httpServiceProvider = NullHttpServiceProvider.getInstance();
        httpServer.initialize(httpServiceProvider);
        assertTrue(httpServer.startServer(0, "localhost"));
        int port = httpServer.getListeningPort();
        HttpResponse httpResponse = send(getUrl(port, HttpRouter.HEARTBEAT), HttpServer.Method.GET);
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
        HttpResponse httpResponse = send(getUrl(port, HttpRouter.METRICS), HttpServer.Method.GET);
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpResponse.responseCode);
        httpServer.stopServer();
    }

    @Test
    public void testHttpMethods() throws Exception {
        VertxHttpServer httpServer = new VertxHttpServer();
        HttpServiceProvider httpServiceProvider = NullHttpServiceProvider.getInstance();
        httpServer.initialize(httpServiceProvider);
        assertTrue(httpServer.startServer(0));
        int port = httpServer.getListeningPort();
        HttpResponse httpResponse = send(getUrl(port, HttpRouter.GC), HttpServer.Method.GET);
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpResponse.responseCode);
        httpResponse = send(getUrl(port, HttpRouter.GC), HttpServer.Method.POST);
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpResponse.responseCode);
        httpResponse = send(getUrl(port, HttpRouter.GC), HttpServer.Method.PUT);
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpResponse.responseCode);
        httpServer.stopServer();
    }

    @Test
    public void testHttpMethodsWithBody() throws IOException {
        VertxHttpServer httpServer = new VertxHttpServer();
        HttpServiceProvider httpServiceProvider = NullHttpServiceProvider.getInstance();
        httpServer.initialize(httpServiceProvider);
        assertTrue(httpServer.startServer(0));
        int port = httpServer.getListeningPort();
        String body = "{\"bookie_src\": \"localhost:3181\"}";
        HttpResponse httpResponse = send(getUrl(port, HttpRouter.DECOMMISSION), HttpServer.Method.PUT, body);
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpResponse.responseCode);
        assertEquals(body, httpResponse.responseBody);
        httpServer.stopServer();
    }

    @Test
    public void testArbitraryFileUpload() throws IOException {
        VertxHttpServer httpServer = new VertxHttpServer();
        HttpServiceProvider httpServiceProvider = NullHttpServiceProvider.getInstance();
        httpServer.initialize(httpServiceProvider);
        assertTrue(httpServer.startServer(0));
        int port = httpServer.getListeningPort();
        File tempFile = File.createTempFile("test-" + System.currentTimeMillis(), null);
        Files.asCharSink(tempFile, StandardCharsets.UTF_8).write(TestVertxHttpServer.class.getName());
        String[] filenamesBeforeUploadRequest = listFiles(BodyHandler.DEFAULT_UPLOADS_DIRECTORY);
        HttpResponse httpResponse = sendFile(getUrl(port, HttpRouter.BOOKIE_INFO), tempFile);
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpResponse.responseCode);
        assertArrayEquals(filenamesBeforeUploadRequest, listFiles(BodyHandler.DEFAULT_UPLOADS_DIRECTORY));
        httpServer.stopServer();
    }

    private HttpResponse send(String url, HttpServer.Method method) throws IOException {
        return send(url, method, "");
    }

    // HTTP request
    private HttpResponse send(String url, HttpServer.Method method, String body) throws IOException {
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        // optional, default is GET
        con.setRequestMethod(method.toString());
        if (body != "") {
            con.setDoOutput(true);
            con.setFixedLengthStreamingMode(body.length());
            OutputStream outputStream = con.getOutputStream();
            outputStream.write(body.getBytes(StandardCharsets.UTF_8));
            outputStream.flush();
        }
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

    private HttpResponse sendFile(String url, File file) throws IOException {
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        String boundary = "---------------------------" + System.currentTimeMillis();
        // optional, default is GET
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
        try (
                OutputStream outputStream = con.getOutputStream();
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8),
                        true);
                FileInputStream fileInputStream = new FileInputStream(file);
        ) {
            writer.append("--" + boundary).append("\r\n");
            writer.append("Content-Disposition: form-data; name=\"file\"; filename=\"file.txt\"").append("\r\n");
            writer.append("Content-Type: text/plain").append("\r\n");
            writer.append("\r\n");

            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }

            writer.append("\r\n");
            writer.append("--" + boundary + "--").append("\r\n");
        }
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

    private String[] listFiles(String directory) {
        File directoryFile = new File(directory);
        if (!directoryFile.exists() || !directoryFile.isDirectory()) {
            return new String[0];
        }
        return directoryFile.list();
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
