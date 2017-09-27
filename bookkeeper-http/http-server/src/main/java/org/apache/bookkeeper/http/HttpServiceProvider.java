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

import org.apache.bookkeeper.http.service.HttpEndpointService;

/**
 * Provider to provide services for different http endpoint handlers.
 */
public interface HttpServiceProvider {

    /**
     * Provide heartbeat service for heartbeat api.
     */
    HttpEndpointService provideHeartbeatService();

    //
    // config
    //

    /**
     * Provide service for configuration api.
     */
    HttpEndpointService provideConfigurationService();

    //
    // ledger
    //

    /**
     * Provide service for delete ledger api.
     */
    HttpEndpointService provideDeleteLedgerService();

    /**
     * Provide service for list ledger api.
     */
    HttpEndpointService provideListLedgerService();

    /**
     * Provide service for delete ledger api.
     */
    HttpEndpointService provideGetLedgerMetaService();

    /**
     * Provide service for read ledger entries api.
     */
    HttpEndpointService provideReadLedgerEntryService();

    //
    // bookie
    //

    /**
     * Provide service for list bookies api.
     */
    HttpEndpointService provideListBookiesService();

    /**
     * Provide service for list bookie disk usage api.
     */
    HttpEndpointService provideListBookieInfoService();

    /**
     * Provide service for get last log mark api.
     */
    HttpEndpointService provideGetLastLogMarkService();

    /**
     * Provide service for list bookie disk files api.
     */
    HttpEndpointService provideListDiskFileService();

    /**
     * Provide service for expand bookie storage api.
     */
    HttpEndpointService provideExpandStorageService();

    //
    // autorecovery
    //

    /**
     * Provide service for auto recovery failed bookie api.
     */
    HttpEndpointService provideRecoveryBookieService();

    /**
     * Provide service for get auditor api.
     */
    HttpEndpointService provideWhoIsAuditorService();

    /**
     * Provide service for list under replicated ledger api.
     */
    HttpEndpointService provideListUnderReplicatedLedgerService();

    /**
     * Provide service for trigger audit api.
     */
    HttpEndpointService provideTriggerAuditService();

    /**
     * Provide service for set/get lostBookieRecoveryDelay api.
     */
    HttpEndpointService provideLostBookieRecoveryDelayService();

    /**
     * Provide service for decommission bookie api.
     */
    HttpEndpointService provideDecommissionService();

}
