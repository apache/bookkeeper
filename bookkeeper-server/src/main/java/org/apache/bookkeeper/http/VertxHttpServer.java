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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.bookkeeper.http.handler.AbstractHandlerFactory;
import org.apache.bookkeeper.http.handler.VertxHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class VertxHttpServer implements HttpServer {

    private final Logger LOG = LoggerFactory.getLogger(VertxHttpServer.class);

    private Vertx vertx;
    private boolean isRunning;
    private ServerOptions serverOptions;

    public VertxHttpServer() {
        this.serverOptions = new ServerOptions();
        this.vertx = Vertx.vertx();
    }

    @Override
    public void initialize(ServerOptions serverOptions) {
        this.serverOptions = serverOptions;
    }

    @Override
    public void startServer() {
        int port = serverOptions.getPort();
        CompletableFuture<AsyncResult> future = new CompletableFuture<>();
        AbstractHandlerFactory<Handler<RoutingContext>> handlerFactory = new VertxHandlerFactory(serverOptions);
        Router router = Router.router(vertx);
        router.get(HEARTBEAT).handler(handlerFactory.newHeartbeatHandler());
        router.get(SERVER_CONFIG).handler(handlerFactory.newConfigurationHandler());
        router.get(BOOKIE_STATUS).handler(handlerFactory.newBookieStatusHandler());
        vertx.deployVerticle(new AbstractVerticle() {
            @Override
            public void start() throws Exception {
                LOG.info("Starting Vertx HTTP server on port {}", port);
                vertx.createHttpServer().requestHandler(router::accept).listen(port, asyncResult -> {
                    isRunning = true;
                    future.complete(asyncResult);
                });
            }
        });
        try {
            AsyncResult asyncResult = future.get();
            if (asyncResult.succeeded()) {
                LOG.info("Http server started successfully");
            } else {
                LOG.error("Failed to start http server on port {}", port, asyncResult.cause());
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to start http server on port {}", port, e);
        }
    }

    @Override
    public void stopServer() {
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Vertx vertx = Vertx.vertx();
        vertx.close(asyncResult -> {
            isRunning = false;
            shutdownLatch.countDown();
            LOG.info("HTTP server is shutdown");
        });
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while shutting down http server");
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }
}
