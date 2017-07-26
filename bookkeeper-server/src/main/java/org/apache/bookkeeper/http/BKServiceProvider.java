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

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.service.HeartbeatService;
import org.apache.bookkeeper.http.service.Service;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.Auditor;
import org.apache.bookkeeper.replication.AutoRecoveryMain;

/**
 * Bookkeeper based implementation of ServiceProvider,
 * which provide bookkeeper services to handle http requests
 * from different http endpoints.
 */
public class BKServiceProvider implements ServiceProvider {

    private final BookieServer bookieServer;
    private final AutoRecoveryMain autoRecovery;
    private final ServerConfiguration serverConf;

    private BKServiceProvider(BookieServer bookieServer,
                             AutoRecoveryMain autoRecovery,
                             ServerConfiguration serverConf) {
        this.bookieServer = bookieServer;
        this.autoRecovery = autoRecovery;
        this.serverConf = serverConf;
    }

    @Override
    public Service provideHeartbeatService() {
        return new HeartbeatService();
    }

    @Override
    public Service provideConfigurationService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return NullServiceProvider.NULL_SERVICE;
        }
        return new ConfigurationService(configuration);
    }

    private ServerConfiguration getServerConf() {
        return serverConf;
    }

    private Auditor getAuditor() {
        return autoRecovery == null ? null : autoRecovery.getAuditor();
    }

    private Bookie getBookie() {
        return bookieServer == null ? null : bookieServer.getBookie();
    }

    /**
     * Builder for ServiceProvider.
     */
    public static class Builder {

        BookieServer bookieServer = null;
        AutoRecoveryMain autoRecovery = null;
        ServerConfiguration serverConf = null;

        public Builder setBookieServer(BookieServer bookieServer) {
            this.bookieServer = bookieServer;
            return this;
        }

        public Builder setAutoRecovery(AutoRecoveryMain autoRecovery) {
            this.autoRecovery = autoRecovery;
            return this;
        }

        public Builder setServerConfiguration(ServerConfiguration conf) {
            this.serverConf = conf;
            return this;
        }

        public BKServiceProvider build() {
            return new BKServiceProvider(
                bookieServer,
                autoRecovery,
                serverConf
            );
        }
    }

}
