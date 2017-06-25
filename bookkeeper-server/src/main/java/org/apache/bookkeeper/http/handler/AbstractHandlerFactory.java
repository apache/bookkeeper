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

package org.apache.bookkeeper.http.handler;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.ServerOptions;
import org.apache.bookkeeper.http.service.BookieStatusService;
import org.apache.bookkeeper.http.service.ConfigService;
import org.apache.bookkeeper.http.service.HeartbeatService;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.AutoRecoveryMain;

public abstract class AbstractHandlerFactory<Handler> {

    private AutoRecoveryMain autoRecovery;
    private ServerConfiguration serverConfiguration;
    private BookieServer bookieServer;

    public AbstractHandlerFactory(ServerOptions serverOptions) {
        this.autoRecovery = serverOptions.getAutoRecovery();
        this.serverConfiguration = serverOptions.getServerConf();
        this.bookieServer = serverOptions.getBookieServer();
    }

    /**
     * Create a handler for heartbeat service
     */
    public abstract Handler newHeartbeatHandler();

    /**
     * Create a handler for configuration service
     */
    public abstract Handler newConfigurationHandler();

    /**
     * Create a handler for bookie status service
     */
    public abstract Handler newBookieStatusHandler();

    HeartbeatService getHeartbeatService() {
        return new HeartbeatService();
    }

    ConfigService getConfigService() {
        if (serverConfiguration == null) {
            return null;
        }
        return new ConfigService(serverConfiguration);
    }

    BookieStatusService getBookieStatusService() {
        if (bookieServer == null || bookieServer.getBookie() == null) {
            return null;
        }
        return new BookieStatusService(bookieServer.getBookie());
    }
}
