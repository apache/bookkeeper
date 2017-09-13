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

import org.apache.bookkeeper.http.service.HttpService;

/**
 * Provider to provide services for different http endpoint handlers.
 */
public interface HttpServiceProvider {

    /**
     * Provide heartbeat service for heartbeat api.
     */
    HttpService provideHeartbeatService();

    //
    // config
    //

    /**
     * Provide service for configuration api.
     */
    HttpService provideConfigurationService();

    //
    // bookkeeper
    //

    /**
     * Provide service for list bookies api.
     */
    HttpService provideListBookiesService();

    /**
     * Provide service for update cookie api.
     */
    HttpService provideUpdataCookieService();

    //
    // ledger
    //

    /**
     * Provide service for delete ledger api.
     */
    HttpService provideDeleteLedgerService();

    /**
     * Provide service for list ledger api.
     */
    HttpService provideListLedgerService();

    /**
     * Provide service for delete ledger api.
     */
    HttpService provideGetLedgerMetaService();

    /**
     * Provide service for read ledger entries api.
     */
    HttpService provideReadLedgerEntryService();

    //
    // bookie
    //

    /**
     * Provide service for list bookie disk usage api.
     */
    HttpService provideListBookieInfoService();

    /**
     * Provide service for get last log mark api.
     */
    HttpService provideGetLastLogMarkService();

    /**
     * Provide service for list bookie disk files api.
     */
    HttpService provideListDiskFileService();

    /**
     * Provide service for read entry log api.
     */
    HttpService provideReadEntryLogService();

    /**
     * Provide service for read journal file api.
     */
    HttpService provideReadJournalFileService();

    /**
     * Provide service for expend bookie storage api.
     */
    HttpService provideExpendStorageService();

    //
    // autorecovery
    //

    /**
     * Provide service for auto recovery failed bookie api.
     */
    HttpService provideAutoRecoveryBookieService();

    /**
     * Provide service for get auditor api.
     */
    HttpService provideWhoIsAuditorService();

    /**
     * Provide service for list under replicated ledger api.
     */
    HttpService provideListUnderReplicatedLedgerService();

    /**
     * Provide service for trigger audit api.
     */
    HttpService provideTriggerAuditService();

    /**
     * Provide service for set/get lostBookieRecoveryDelay api.
     */
    HttpService provideLostBookieRecoveryDelayService();

    /**
     * Provide service for decommission bookie api.
     */
    HttpService provideDecommissionService();

}
