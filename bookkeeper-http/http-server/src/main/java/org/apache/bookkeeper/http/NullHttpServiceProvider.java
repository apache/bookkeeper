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

import org.apache.bookkeeper.http.service.HeartbeatService;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.NullHttpService;

/**
 * HttpEndpointService provider which provide service that do nothing.
 */
public class NullHttpServiceProvider implements HttpServiceProvider {

    private static final NullHttpServiceProvider NULL_HTTP_SERVICE_PROVIDER = new NullHttpServiceProvider();

    static final HttpEndpointService NULL_HTTP_SERVICE = new NullHttpService();

    public static NullHttpServiceProvider getInstance() {
        return NULL_HTTP_SERVICE_PROVIDER;
    }

    @Override
    public HttpEndpointService provideHeartbeatService() {
        return new HeartbeatService();
    }

    @Override
    public HttpEndpointService provideConfigurationService() {
        return NULL_HTTP_SERVICE;
    }

    //
    // ledger
    //

    /**
     * Provide service for delete ledger api.
     */
    @Override
    public HttpEndpointService provideDeleteLedgerService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for list ledger api.
     */
    @Override
    public HttpEndpointService provideListLedgerService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for delete ledger api.
     */
    @Override
    public HttpEndpointService provideGetLedgerMetaService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for read ledger entries api.
     */
    @Override
    public HttpEndpointService provideReadLedgerEntryService() {
        return NULL_HTTP_SERVICE;
    }

    //
    // bookie
    //

    /**
     * Provide service for list bookies api.
     */
    @Override
    public HttpEndpointService provideListBookiesService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for list bookie disk usage api.
     */
    @Override
    public HttpEndpointService provideListBookieInfoService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for get last log mark api.
     */
    @Override
    public HttpEndpointService provideGetLastLogMarkService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for list bookie disk files api.
     */
    @Override
    public HttpEndpointService provideListDiskFileService() {
        return NULL_HTTP_SERVICE;
    }

  /**
     * Provide service for expand bookie storage api.
     */
    @Override
    public HttpEndpointService provideExpandStorageService() {
        return NULL_HTTP_SERVICE;
    }

    //
    // autorecovery
    //

    /**
     * Provide service for auto recovery failed bookie api.
     */
    @Override
    public HttpEndpointService provideRecoveryBookieService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for get auditor api.
     */
    @Override
    public HttpEndpointService provideWhoIsAuditorService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for list under replicated ledger api.
     */
    @Override
    public HttpEndpointService provideListUnderReplicatedLedgerService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for trigger audit api.
     */
    @Override
    public HttpEndpointService provideTriggerAuditService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for set/get lostBookieRecoveryDelay api.
     */
    @Override
    public HttpEndpointService provideLostBookieRecoveryDelayService() {
        return NULL_HTTP_SERVICE;
    }

    /**
     * Provide service for decommission bookie api.
     */
    @Override
    public HttpEndpointService provideDecommissionService() {
        return NULL_HTTP_SERVICE;
    }
}
