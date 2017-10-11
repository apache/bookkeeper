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

package org.apache.bookkeeper.http.twitter;

import com.twitter.finagle.Http;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.http.HttpMuxer;
import com.twitter.finagle.http.HttpMuxer$;
import com.twitter.server.AbstractTwitterServer;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.bookkeeper.http.HttpRouter;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TwitterServer implementation of Http Server.
 */
public class TwitterHttpServer extends AbstractTwitterServer implements HttpServer {

    static final Logger LOG = LoggerFactory.getLogger(TwitterHttpServer.class);

    private ListeningServer server;
    private boolean isRunning;
    private int port;
    private HttpServiceProvider httpServiceProvider;

    @Override
    public void initialize(HttpServiceProvider httpServiceProvider) {
        this.httpServiceProvider = httpServiceProvider;
    }

    @Override
    public boolean startServer(int port) {
        try {
            this.port = port;
            this.main();
            isRunning = true;
            LOG.info("Twitter HTTP server started successfully");
            return true;
        } catch (Throwable throwable) {
            LOG.error("Failed to start Twitter Http Server", throwable);
        }
        return false;
    }

    @Override
    public void stopServer() {
        try {
            httpServiceProvider.close();
        } catch (IOException ioe) {
            LOG.error("Error while close httpServiceProvider", ioe);
        }
        if (server != null) {
            server.close();
            isRunning = false;
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public void main() throws Throwable {
        LOG.info("Starting Twitter HTTP server on port {}", port);
        TwitterHttpHandlerFactory handlerFactory = new TwitterHttpHandlerFactory(httpServiceProvider);
        HttpRouter<TwitterAbstractHandler> requestRouter = new HttpRouter<TwitterAbstractHandler>(handlerFactory) {
            @Override
            public void bindHandler(String endpoint, TwitterAbstractHandler handler) {
                HttpMuxer.addHandler(endpoint, handler);
            }
        };
        requestRouter.bindAll();
        InetSocketAddress addr = new InetSocketAddress(port);
        server = Http.server().serve(addr, HttpMuxer$.MODULE$);
    }

    @Override
    public void onExit() {
        stopServer();
    }

}
