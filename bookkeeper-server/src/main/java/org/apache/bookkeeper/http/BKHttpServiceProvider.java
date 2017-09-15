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

import static org.apache.bookkeeper.http.NullHttpServiceProvider.NULL_HTTP_SERVICE;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.service.ErrorHttpService;
import org.apache.bookkeeper.http.service.HeartbeatService;
import org.apache.bookkeeper.http.service.HttpService;
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
    public HttpService provideHeartbeatService() {
        return new HeartbeatService();
    }

    @Override
    public HttpService provideConfigurationService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new ConfigurationService(configuration);
    }

    // TODO

    //
    // bookkeeper
    //

    /**
     * Provide service for list bookies api.
     */
    @Override
    public HttpService provideListBookiesService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new ListBookiesService(configuration);
    }

    /**
     * Provide service for update cookie api.
     */
    @Override
    public HttpService provideUpdataCookieService() {
        return NULL_HTTP_SERVICE;
    }

    //
    // ledger
    //

    /**
     * Provide service for delete ledger api.
     */
    @Override
    public HttpService provideDeleteLedgerService() {
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
    public HttpService provideListLedgerService() {
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
    public HttpService provideGetLedgerMetaService() {
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
    public HttpService provideReadLedgerEntryService() {
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
     * Provide service for list bookie disk usage api.
     */
    @Override
    public HttpService provideListBookieInfoService() {
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
    public HttpService provideGetLastLogMarkService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for list bookie disk files api.
     */
    @Override
    public HttpService provideListDiskFileService() {
        ServerConfiguration configuration = getServerConf();
        if (configuration == null) {
            return new ErrorHttpService();
        }
        return new ListDiskFilesService(configuration);
    }

    /**
     * Provide service for read entry log api.
     */
    @Override
    public HttpService provideReadEntryLogService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for read journal file api.
     */
    @Override
    public HttpService provideReadJournalFileService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for expend bookie storage api.
     */
    @Override
    public HttpService provideExpendStorageService() {
        return NULL_HTTP_SERVICE;
    }

    //
    // autorecovery
    //

    /**
     * Provide service for auto recovery failed bookie api.
     */
    @Override
    public HttpService provideAutoRecoveryBookieService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for get auditor api.
     */
    @Override
    public HttpService provideWhoIsAuditorService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for list under replicated ledger api.
     */
    @Override
    public HttpService provideListUnderReplicatedLedgerService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for trigger audit api.
     */
    @Override
    public HttpService provideTriggerAuditService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for set/get lostBookieRecoveryDelay api.
     */
    @Override
    public HttpService provideLostBookieRecoveryDelayService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for decommission bookie api.
     */
    @Override
    public HttpService provideDecommissionService() {
        return NULL_HTTP_SERVICE;
    }

}
