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

/**
 * Abstract handler factory which provide interface
 * to create handlers for bookkeeper http endpoints.
 */
public abstract class AbstractHttpHandlerFactory<Handler> {
    private HttpServiceProvider httpServiceProvider;

    public AbstractHttpHandlerFactory(HttpServiceProvider httpServiceProvider) {
        this.httpServiceProvider = httpServiceProvider;
    }

    public HttpServiceProvider getHttpServiceProvider() {
        return httpServiceProvider;
    }

    /**
     * Create a handler for heartbeat api.
     */
    public abstract Handler newHeartbeatHandler();

    /**
     * Create a handler for server configuration api.
     */
    public abstract Handler newConfigurationHandler();

    //
    // ledger
    //

    /**
     * Create a handler for delete ledger api.
     */
    public abstract Handler newDeleteLedgerHandler();

    /**
     * Create a handler for list ledger api.
     */
    public abstract Handler newListLedgerHandler();

    /**
     * Create a handler for delete ledger api.
     */
    public abstract Handler newGetLedgerMetaHandler();

    /**
     * Create a handler for read ledger entries api.
     */
    public abstract Handler newReadLedgerEntryHandler();

    //
    // bookie
    //

    /**
     * Create a handler for list bookies api.
     */
    public abstract Handler newListBookiesHandler();

    /**
     * Create a handler for list bookie disk usage api.
     */
    public abstract Handler newListBookieInfoHandler();

    /**
     * Create a handler for get last log mark api.
     */
    public abstract Handler newGetLastLogMarkHandler();

    /**
     * Create a handler for list bookie disk files api.
     */
    public abstract Handler newListDiskFileHandler();

    /**
     * Create a handler for expand bookie storage api.
     */
    public abstract Handler newExpandStorageHandler();

    //
    // autorecovery
    //

    /**
     * Create a handler for auto recovery failed bookie api.
     */
    public abstract Handler newRecoveryBookieHandler();

    /**
     * Create a handler for get auditor api.
     */
    public abstract Handler newWhoIsAuditorHandler();

    /**
     * Create a handler for list under replicated ledger api.
     */
    public abstract Handler newListUnderReplicatedLedgerHandler();

    /**
     * Create a handler for trigger audit api.
     */
    public abstract Handler newTriggerAuditHandler();

    /**
     * Create a handler for set/get lostBookieRecoveryDelay api.
     */
    public abstract Handler newLostBookieRecoveryDelayHandler();

    /**
     * Create a handler for decommission bookie api.
     */
    public abstract Handler newDecommissionHandler();

}
