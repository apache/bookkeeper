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
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@link ServerLifecycleComponent} that starts the core bookie server.
 */
public class BookieService extends ServerLifecycleComponent {

    public static final String NAME = "bookie-server";

    private final BookieServer server;

    public BookieService(BookieConfiguration conf,
                         StatsLogger statsLogger)
            throws Exception {
        super(NAME, conf, statsLogger);
        this.server = new BookieServer(conf.getServerConf(), statsLogger);
    }

    @Override
    public void setExceptionHandler(UncaughtExceptionHandler handler) {
        server.setExceptionHandler(handler);
    }

    public BookieServer getServer() {
        return server;
    }

    @Override
    protected void doStart() {
        try {
            this.server.start();
        } catch (IOException | UnavailableException | InterruptedException | BKException e) {
            throw new RuntimeException("Failed to start bookie server", e);
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
}
