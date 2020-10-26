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

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.ComponentInfoPublisher;
import org.apache.bookkeeper.common.component.ComponentInfoPublisher.EndpointInfo;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@link ServerLifecycleComponent} that starts the core bookie server.
 */
@Slf4j
public class BookieService extends ServerLifecycleComponent {

    public static final String NAME = "bookie-server";

    private final BookieServer server;

    public BookieService(BookieConfiguration conf,
                         StatsLogger statsLogger,
                         Supplier<BookieServiceInfo> bookieServiceInfoProvider)
            throws Exception {
        super(NAME, conf, statsLogger);
        this.server = new BookieServer(conf.getServerConf(), statsLogger, bookieServiceInfoProvider);
    }

    @Override
    public void setExceptionHandler(UncaughtExceptionHandler handler) {
        super.setExceptionHandler(handler);
        server.setExceptionHandler(handler);
    }

    public BookieServer getServer() {
        return server;
    }

    @Override
    protected void doStart() {
        try {
            this.server.start();
        } catch (InterruptedException exc) {
            throw new RuntimeException("Failed to start bookie server", exc);
        }
    }

    @Override
    protected void doStop() {
        // no-op
    }

    @Override
    protected void doClose() throws IOException {
        this.server.shutdown();
    }

    @Override
    public void publishInfo(ComponentInfoPublisher componentInfoPublisher) {
        try {
            BookieSocketAddress localAddress = getServer().getLocalAddress();
            List<String> extensions = new ArrayList<>();
            if (conf.getServerConf().getTLSProviderFactoryClass() != null) {
                extensions.add("tls");
            }
            EndpointInfo endpoint = new EndpointInfo("bookie",
                    localAddress.getPort(),
                    localAddress.getHostName(),
                    "bookie-rpc", null, extensions);
            componentInfoPublisher.publishEndpoint(endpoint);

        } catch (UnknownHostException err) {
            log.error("Cannot compute local address", err);
        }
    }

}
