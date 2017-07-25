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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;

/**
 * Vertx.io implementation of Http Server.
 */
public class VertxHttpServer implements HttpServer {

    static final Logger LOG = LoggerFactory.getLogger(VertxHttpServer.class);

    private Vertx vertx;
    private boolean isRunning;
    private ServiceProvider serviceProvider;

    public VertxHttpServer() {
        this.vertx = Vertx.vertx();
    }

    @Override
    public void initialize(ServiceProvider serviceProvider) {
        this.serviceProvider = serviceProvider;
    }

    @Override
    public void startServer(int port) {
        CompletableFuture<AsyncResult> future = new CompletableFuture<>();
        VertxHandlerFactory handlerFactory = new VertxHandlerFactory(serviceProvider);
        Router router = Router.router(vertx);
        router.get(HEARTBEAT).handler(handlerFactory.newHeartbeatHandler());
        router.get(SERVER_CONFIG).handler(handlerFactory.newConfigurationHandler());
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
                LOG.info("Vertx Http server started successfully");
            } else {
                LOG.error("Failed to start org.apache.bookkeeper.http server on port {}", port, asyncResult.cause());
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to start org.apache.bookkeeper.http server on port {}", port, e);
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
            LOG.error("Interrupted while shutting down org.apache.bookkeeper.http server");
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }
}
