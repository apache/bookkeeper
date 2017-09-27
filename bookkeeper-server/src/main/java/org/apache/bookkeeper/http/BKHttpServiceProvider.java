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
import org.apache.bookkeeper.http.service.ErrorHttpService;
import org.apache.bookkeeper.http.service.HeartbeatService;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.Auditor;
import org.apache.bookkeeper.replication.AutoRecoveryMain;

/**
 * Bookkeeper based implementation of HttpServiceProvider,
 * which provide bookkeeper services to handle http requests
 * from different http endpoints.
 */
public class BKHttpServiceProvider implements HttpServiceProvider {

    private final BookieServer bookieServer;
    private final AutoRecoveryMain autoRecovery;
    private final ServerConfiguration serverConf;

    private BKHttpServiceProvider(BookieServer bookieServer,
                                  AutoRecoveryMain autoRecovery,
                                  ServerConfiguration serverConf) {
        this.bookieServer = bookieServer;
        this.autoRecovery = autoRecovery;
        this.serverConf = serverConf;
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
     * Builder for HttpServiceProvider.
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

        public BKHttpServiceProvider build() {
            return new BKHttpServiceProvider(
                bookieServer,
                autoRecovery,
                serverConf
            );
        }
    }

    @Override
    public HttpEndpointService provideHeartbeatService() {
        return new HeartbeatService();
    }

    @Override
    public HttpEndpointService provideConfigurationService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new ConfigurationService(configuration);
    }

    // TODO

    //
    // ledger
    //

    /**
     * Provide service for delete ledger api.
     */
    @Override
    public HttpEndpointService provideDeleteLedgerService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new DeleteLedgerService(configuration);
    }

    /**
     * Provide service for list ledger api.
     */
    @Override
    public HttpEndpointService provideListLedgerService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new ListLedgerService(configuration);
    }

    /**
     * Provide service for delete ledger api.
     */
    @Override
    public HttpEndpointService provideGetLedgerMetaService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new GetLedgerMetaService(configuration);
    }

    /**
     * Provide service for read ledger entries api.
     */
    @Override
    public HttpEndpointService provideReadLedgerEntryService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new ReadLedgerEntryService(configuration);
    }

    //
    // bookie
    //

    /**
     * Provide service for list bookies api.
     */
    @Override
    public HttpEndpointService provideListBookiesService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new ListBookiesService(configuration);
    }

    /**
     * Provide service for list bookie disk usage api.
     */
    @Override
    public HttpEndpointService provideListBookieInfoService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new ListBookieInfoService(configuration);
    }

    /**
     * Provide service for get last log mark api.
     */
    @Override
    public HttpEndpointService provideGetLastLogMarkService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new GetLastLogMarkService(configuration);
    }

    /**
     * Provide service for list bookie disk files api.
     */
    @Override
    public HttpEndpointService provideListDiskFileService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new ListDiskFilesService(configuration);
    }

    /**
     * Provide service for expand bookie storage api.
     */
    @Override
    public HttpEndpointService provideExpandStorageService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new ExpandStorageService(configuration);
    }

    //
    // autorecovery
    //

    /**
     * Provide service for auto recovery failed bookie api.
     */
    @Override
    public HttpEndpointService provideRecoveryBookieService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new RecoveryBookieService(configuration);
    }

    /**
     * Provide service for get auditor api.
     */
    @Override
    public HttpEndpointService provideWhoIsAuditorService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new WhoIsAuditorService(configuration);
    }

    /**
     * Provide service for list under replicated ledger api.
     */
    @Override
    public HttpEndpointService provideListUnderReplicatedLedgerService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new ListUnderReplicatedLedgerService(configuration);
    }

    /**
     * Provide service for trigger audit api.
     */
    @Override
    public HttpEndpointService provideTriggerAuditService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new TriggerAuditService(configuration);
    }

    /**
     * Provide service for set/get lostBookieRecoveryDelay api.
     */
    @Override
    public HttpEndpointService provideLostBookieRecoveryDelayService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new LostBookieRecoveryDelayService(configuration);
    }

    /**
     * Provide service for decommission bookie api.
     */
    @Override
    public HttpEndpointService provideDecommissionService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new DecommissionService(configuration);
    }

}
