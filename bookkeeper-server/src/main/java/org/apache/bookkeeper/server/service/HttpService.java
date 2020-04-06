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

package org.apache.bookkeeper.server.service;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import org.apache.bookkeeper.common.component.ComponentInfoPublisher;

import org.apache.bookkeeper.common.component.ComponentInfoPublisher.EndpointInfo;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServerLoader;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.http.BKHttpServiceProvider;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@link ServerLifecycleComponent} that runs http service.
 */
public class HttpService extends ServerLifecycleComponent {

    public static final String NAME = "http-service";

    private HttpServer server;

    public HttpService(BKHttpServiceProvider provider,
                       BookieConfiguration conf,
                       StatsLogger statsLogger) {
        super(NAME, conf, statsLogger);

        HttpServerLoader.loadHttpServer(conf.getServerConf());
        server = HttpServerLoader.get();
        checkNotNull(server);
        server.initialize(provider);
    }

    @Override
    protected void doStart() {
        server.startServer(conf.getServerConf().getHttpServerPort());
    }

    @Override
    protected void doStop() {
        // no-op
    }

    @Override
    protected void doClose() throws IOException {
        server.stopServer();
    }

    @Override
    public void publishInfo(ComponentInfoPublisher componentInfoPublisher) {
        if (conf.getServerConf().isHttpServerEnabled()) {
            EndpointInfo endpoint = new EndpointInfo("httpserver",
                    conf.getServerConf().getHttpServerPort(),
                    "0.0.0.0",
                    "http", null, null);
            componentInfoPublisher.publishEndpoint(endpoint);
        }
    }

}
