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
package org.apache.bookkeeper.http.twitter;

import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.util.Future;

import org.apache.bookkeeper.http.AbstractHttpHandlerFactory;
import org.apache.bookkeeper.http.HttpServiceProvider;



/**
 * Factory which provide http handlers for TwitterServer based Http Server.
 */
public class TwitterHttpHandlerFactory extends AbstractHttpHandlerFactory<TwitterAbstractHandler> {

    public TwitterHttpHandlerFactory(HttpServiceProvider httpServiceProvider) {
        super(httpServiceProvider);
    }

    @Override
    public TwitterAbstractHandler newHeartbeatHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideHeartbeatService(), request);
            }
        };
    }

    @Override
    public TwitterAbstractHandler newConfigurationHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideConfigurationService(), request);
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
    public TwitterAbstractHandler newListBookiesHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideListBookiesService(), request);
            }
        };
    }

    /**
     * Create a handler for update cookie api.
     */
    @Override
    public TwitterAbstractHandler newUpdataCookieHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideUpdataCookieService(), request);
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
    public TwitterAbstractHandler newDeleteLedgerHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideDeleteLedgerService(), request);
            }
        };
    }

    /**
     * Create a handler for list ledger api.
     */
    @Override
    public TwitterAbstractHandler newListLedgerHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideListLedgerService(), request);
            }
        };
    }

    /**
     * Create a handler for delete ledger api.
     */
    @Override
    public TwitterAbstractHandler newGetLedgerMetaHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideGetLedgerMetaService(), request);
            }
        };
    }

    /**
     * Create a handler for read ledger entries api.
     */
    @Override
    public TwitterAbstractHandler newReadLedgerEntryHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideReadLedgerEntryService(), request);
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
    public TwitterAbstractHandler newListBookieInfoHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideListBookieInfoService(), request);
            }
        };
    }

    /**
     * Create a handler for get last log mark api.
     */
    @Override
    public TwitterAbstractHandler newGetLastLogMarkHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideGetLastLogMarkService(), request);
            }
        };
    }

    /**
     * Create a handler for list bookie disk files api.
     */
    @Override
    public TwitterAbstractHandler newListDiskFileHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideListDiskFileService(), request);
            }
        };
    }

    /**
     * Create a handler for read entry log api.
     */
    @Override
    public TwitterAbstractHandler newReadEntryLogHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideReadEntryLogService(), request);
            }
        };
    }

    /**
     * Create a handler for read journal file api.
     */
    @Override
    public TwitterAbstractHandler newReadJournalFileHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideReadJournalFileService(), request);
            }
        };
    }

    /**
     * Create a handler for expend bookie storage api.
     */
    @Override
    public TwitterAbstractHandler newExpendStorageHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideExpendStorageService(), request);
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
    public TwitterAbstractHandler newAutoRecoveryBookieHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideAutoRecoveryBookieService(), request);
            }
        };
    }

    /**
     * Create a handler for get auditor api.
     */
    @Override
    public TwitterAbstractHandler newWhoIsAuditorHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideWhoIsAuditorService(), request);
            }
        };
    }

    /**
     * Create a handler for list under replicated ledger api.
     */
    @Override
    public TwitterAbstractHandler newListUnderReplicatedLedgerHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideListUnderReplicatedLedgerService(), request);
            }
        };
    }

    /**
     * Create a handler for trigger audit api.
     */
    @Override
    public TwitterAbstractHandler newTriggerAuditHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideTriggerAuditService(), request);
            }
        };
    }

    /**
     * Create a handler for set/get lostBookieRecoveryDelay api.
     */
    @Override
    public TwitterAbstractHandler newLostBookieRecoveryDelayHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideLostBookieRecoveryDelayService(), request);
            }
        };
    }

    /**
     * Create a handler for decommission bookie api.
     */
    @Override
    public TwitterAbstractHandler newDecommissionHandler() {
        return new TwitterAbstractHandler() {
            @Override
            public Future<Response> apply(Request request) {
                return processRequest(getHttpServiceProvider().provideDecommissionService(), request);
            }
        };
    }

}
