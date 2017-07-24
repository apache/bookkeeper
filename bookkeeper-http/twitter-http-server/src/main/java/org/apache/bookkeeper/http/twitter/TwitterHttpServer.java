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

import java.net.InetSocketAddress;

import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.finagle.Http;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.http.HttpMuxer;
import com.twitter.server.AbstractTwitterServer;

public class TwitterHttpServer extends AbstractTwitterServer implements HttpServer {

    private final Logger LOG = LoggerFactory.getLogger(TwitterHttpServer.class);

    private ListeningServer server;
    private boolean isRunning;
    private int port;
    private ServiceProvider serviceProvider;

    @Override
    public void initialize(ServiceProvider serviceProvider) {
        this.serviceProvider = serviceProvider;
    }

    @Override
    public void startServer(int port) {
        try {
            this.port = port;
            this.main();
            LOG.info("Twitter HTTP server started successfully");
        } catch (Throwable throwable) {
            LOG.error("Failed to start Twitter Http Server", throwable);
        }
    }

    @Override
    public void stopServer() {
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
        TwitterHandlerFactory handlerFactory = new TwitterHandlerFactory(serviceProvider);
        HttpMuxer muxer = new HttpMuxer()
            .withHandler(HEARTBEAT, handlerFactory.newHeartbeatHandler())
            .withHandler(SERVER_CONFIG, handlerFactory.newConfigurationHandler());
        InetSocketAddress addr = new InetSocketAddress(port);
        server = Http.server().serve(addr, muxer);
        isRunning = true;
    }

    @Override
    public void onExit() {
        stopServer();
    }

}
