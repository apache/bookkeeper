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

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.http.HttpRouter;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServerConfiguration;
import org.apache.bookkeeper.http.HttpServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vertx.io implementation of Http Server.
 */
public class VertxHttpServer implements HttpServer {

    static final Logger LOG = LoggerFactory.getLogger(VertxHttpServer.class);

    private final Vertx vertx;
    private boolean isRunning;
    private HttpServiceProvider httpServiceProvider;
    private int listeningPort = -1;

    public VertxHttpServer() {
        this.vertx = Vertx.vertx();
    }

    int getListeningPort() {
        return listeningPort;
    }

    @Override
    public void initialize(HttpServiceProvider httpServiceProvider) {
        this.httpServiceProvider = httpServiceProvider;
    }

    @Override
    public boolean startServer(int port) {
        return startServer(port, "0.0.0.0");
    }

    @Override
    public boolean startServer(int port, String host) {
        return startServer(port, host, new HttpServerConfiguration());
    }

    @Override
    public boolean startServer(int port, String host, HttpServerConfiguration httpServerConfiguration) {
        CompletableFuture<AsyncResult<io.vertx.core.http.HttpServer>> future = new CompletableFuture<>();
        VertxHttpHandlerFactory handlerFactory = new VertxHttpHandlerFactory(httpServiceProvider);
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create(false));
        HttpRouter<VertxAbstractHandler> requestRouter = new HttpRouter<VertxAbstractHandler>(handlerFactory) {
            @Override
            public void bindHandler(String endpoint, VertxAbstractHandler handler) {
                router.get(endpoint).handler(handler);
                router.put(endpoint).handler(handler);
                router.post(endpoint).handler(handler);
                router.delete(endpoint).handler(handler);
            }
        };
        requestRouter.bindAll();
        vertx.deployVerticle(new AbstractVerticle() {
            @Override
            public void start() throws Exception {
                HttpServerOptions httpServerOptions = new HttpServerOptions();
                if (httpServerConfiguration.isTlsEnable()) {
                    httpServerOptions.setSsl(true);
                    httpServerOptions.setClientAuth(ClientAuth.REQUIRED);
                    JksOptions keyStoreOptions = new JksOptions();
                    keyStoreOptions.setPath(httpServerConfiguration.getKeyStorePath());
                    keyStoreOptions.setPassword(httpServerConfiguration.getKeyStorePassword());
                    httpServerOptions.setKeyStoreOptions(keyStoreOptions);
                    JksOptions trustStoreOptions = new JksOptions();
                    trustStoreOptions.setPath(httpServerConfiguration.getTrustStorePath());
                    trustStoreOptions.setPassword(httpServerConfiguration.getTrustStorePassword());
                    httpServerOptions.setTrustStoreOptions(trustStoreOptions);
                }
                LOG.info("Starting Vertx HTTP server on port {}", port);
                vertx.createHttpServer(httpServerOptions).requestHandler(router).listen(port, host, future::complete);
            }
        });
        try {
            AsyncResult<io.vertx.core.http.HttpServer> asyncResult = future.get();
            if (asyncResult.succeeded()) {
                LOG.info("Vertx Http server started successfully");
                listeningPort = asyncResult.result().actualPort();
                isRunning = true;
                return true;
            } else {
                LOG.error("Failed to start org.apache.bookkeeper.http server on port {}", port, asyncResult.cause());
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Failed to start org.apache.bookkeeper.http server on port {}", port, ie);
        } catch (ExecutionException e) {
            LOG.error("Failed to start org.apache.bookkeeper.http server on port {}", port, e);
        }
        return false;
    }

    @Override
    public void stopServer() {
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        try {
            httpServiceProvider.close();
        } catch (IOException ioe) {
            LOG.error("Error while close httpServiceProvider", ioe);
        }
        vertx.close(asyncResult -> {
            isRunning = false;
            shutdownLatch.countDown();
            LOG.info("HTTP server is shutdown");
        });
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while shutting down org.apache.bookkeeper.http server");
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }
}
