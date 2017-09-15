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

import io.vertx.ext.web.RoutingContext;

import org.apache.bookkeeper.http.AbstractHttpHandlerFactory;
import org.apache.bookkeeper.http.HttpServiceProvider;

/**
 * Factory which provide http handlers for Vertx based Http Server.
 */
public class VertxHttpHandlerFactory extends AbstractHttpHandlerFactory<VertxAbstractHandler> {


    public VertxHttpHandlerFactory(HttpServiceProvider httpServiceProvider) {
        super(httpServiceProvider);
    }

    @Override
    public VertxAbstractHandler newHeartbeatHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context) {
                processRequest(getHttpServiceProvider().provideHeartbeatService(), context);
            }
        };
    }

    @Override
    public VertxAbstractHandler newConfigurationHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context) {
                processRequest(getHttpServiceProvider().provideConfigurationService(), context);
            }
        };
    }

    //
    // bookkeeper
    //

    /**
     * Create a handler for list bookies api.
     */
    @Override
    public VertxAbstractHandler newListBookiesHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideListBookiesService(), context);
            }
        };
    }

    /**
     * Create a handler for update cookie api.
     */
    @Override
    public VertxAbstractHandler newUpdataCookieHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideUpdataCookieService(), context);
            }
        };
    }

    //
    // ledger
    //

    /**
     * Create a handler for delete ledger api.
     */
    @Override
    public VertxAbstractHandler newDeleteLedgerHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideDeleteLedgerService(), context);
            }
        };
    }

    /**
     * Create a handler for list ledger api.
     */
    @Override
    public VertxAbstractHandler newListLedgerHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideListLedgerService(), context);
            }
        };
    }

    /**
     * Create a handler for delete ledger api.
     */
    @Override
    public VertxAbstractHandler newGetLedgerMetaHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideGetLedgerMetaService(), context);
            }
        };
    }

    /**
     * Create a handler for read ledger entries api.
     */
    @Override
    public VertxAbstractHandler newReadLedgerEntryHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideReadLedgerEntryService(), context);
            }
        };
    }

    //
    // bookie
    //

    /**
     * Create a handler for list bookie disk usage api.
     */
    @Override
    public VertxAbstractHandler newListBookieInfoHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideListBookieInfoService(), context);
            }
        };
    }

    /**
     * Create a handler for get last log mark api.
     */
    @Override
    public VertxAbstractHandler newGetLastLogMarkHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideGetLastLogMarkService(), context);
            }
        };
    }

    /**
     * Create a handler for list bookie disk files api.
     */
    @Override
    public VertxAbstractHandler newListDiskFileHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideListDiskFileService(), context);
            }
        };
    }

    /**
     * Create a handler for read entry log api.
     */
    @Override
    public VertxAbstractHandler newReadEntryLogHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideReadEntryLogService(), context);
            }
        };
    }

    /**
     * Create a handler for read journal file api.
     */
    @Override
    public VertxAbstractHandler newReadJournalFileHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideReadJournalFileService(), context);
            }
        };
    }

    /**
     * Create a handler for expand bookie storage api.
     */
    @Override
    public VertxAbstractHandler newExpandStorageHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideExpandStorageService(), context);
            }
        };
    }

    //
    // autorecovery
    //

    /**
     * Create a handler for auto recovery failed bookie api.
     */
    @Override
    public VertxAbstractHandler newRecoveryBookieHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideRecoveryBookieService(), context);
            }
        };
    }

    /**
     * Create a handler for get auditor api.
     */
    @Override
    public VertxAbstractHandler newWhoIsAuditorHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideWhoIsAuditorService(), context);
            }
        };
    }

    /**
     * Create a handler for list under replicated ledger api.
     */
    @Override
    public VertxAbstractHandler newListUnderReplicatedLedgerHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideListUnderReplicatedLedgerService(), context);
            }
        };
    }

    /**
     * Create a handler for trigger audit api.
     */
    @Override
    public VertxAbstractHandler newTriggerAuditHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideTriggerAuditService(), context);
            }
        };
    }

    /**
     * Create a handler for set/get lostBookieRecoveryDelay api.
     */
    @Override
    public VertxAbstractHandler newLostBookieRecoveryDelayHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideLostBookieRecoveryDelayService(), context);
            }
        };
    }

    /**
     * Create a handler for decommission bookie api.
     */
    @Override
    public VertxAbstractHandler newDecommissionHandler() {
        return new VertxAbstractHandler() {
            @Override
            public void handle(RoutingContext context){
                processRequest(getHttpServiceProvider().provideDecommissionService(), context);
            }
        };
    }

}
