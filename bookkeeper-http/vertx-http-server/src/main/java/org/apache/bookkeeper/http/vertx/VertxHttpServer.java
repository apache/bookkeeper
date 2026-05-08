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
import lombok.CustomLog;
import org.apache.bookkeeper.http.HttpRouter;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServerConfiguration;
import org.apache.bookkeeper.http.HttpServiceProvider;

/**
 * Vertx.io implementation of Http Server.
 */
@CustomLog
public class VertxHttpServer implements HttpServer {

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
                router.get(endpoint).blockingHandler(handler);
                router.put(endpoint).blockingHandler(handler);
                router.post(endpoint).blockingHandler(handler);
                router.delete(endpoint).blockingHandler(handler);
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
                    httpServerOptions.setKeyCertOptions(keyStoreOptions);
                    JksOptions trustStoreOptions = new JksOptions();
                    trustStoreOptions.setPath(httpServerConfiguration.getTrustStorePath());
                    trustStoreOptions.setPassword(httpServerConfiguration.getTrustStorePassword());
                    httpServerOptions.setTrustOptions(trustStoreOptions);
                }
                log.info().attr("port", port).log("Starting Vertx HTTP server");
                vertx.createHttpServer(httpServerOptions).requestHandler(router).listen(port, host, future::complete);
            }
        });
        try {
            AsyncResult<io.vertx.core.http.HttpServer> asyncResult = future.get();
            if (asyncResult.succeeded()) {
                log.info("Vertx Http server started successfully");
                listeningPort = asyncResult.result().actualPort();
                isRunning = true;
                return true;
            } else {
                log.error().exception(asyncResult.cause()).attr("port", port)
                        .log("Failed to start org.apache.bookkeeper.http server");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error().exception(ie).attr("port", port)
                    .log("Failed to start org.apache.bookkeeper.http server");
        } catch (ExecutionException e) {
            log.error().exception(e).attr("port", port)
                    .log("Failed to start org.apache.bookkeeper.http server");
        }
        return false;
    }

    @Override
    public void stopServer() {
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        try {
            httpServiceProvider.close();
        } catch (IOException ioe) {
            log.error().exception(ioe).log("Error while close httpServiceProvider");
        }
        vertx.close(asyncResult -> {
            isRunning = false;
            shutdownLatch.countDown();
            log.info("HTTP server is shutdown");
        });
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while shutting down org.apache.bookkeeper.http server");
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }
}
