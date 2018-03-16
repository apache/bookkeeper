/*
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
 */
package org.apache.bookkeeper.server.http.service;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Lists;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper expand storage related http request.
 * The PUT method will expand this bookie's storage.
 * User should update the directories info in the conf file with new empty ledger/index
 * directories, before running the command.
 */
public class ExpandStorageService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(ExpandStorageService.class);

    protected ServerConfiguration conf;

    public ExpandStorageService(ServerConfiguration conf) {
        checkNotNull(conf);
        this.conf = conf;
    }

    /*
     * Add new empty ledger/index directories.
     * Update the directories info in the conf file before running the command.
     */
    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.PUT == request.getMethod()) {
            File[] ledgerDirectories = Bookie.getCurrentDirectories(conf.getLedgerDirs());
            File[] journalDirectories = Bookie.getCurrentDirectories(conf.getJournalDirs());
            File[] indexDirectories;
            if (null == conf.getIndexDirs()) {
                indexDirectories = ledgerDirectories;
            } else {
                indexDirectories = Bookie.getCurrentDirectories(conf.getIndexDirs());
            }

            List<File> allLedgerDirs = Lists.newArrayList();
            allLedgerDirs.addAll(Arrays.asList(ledgerDirectories));
            if (indexDirectories != ledgerDirectories) {
                allLedgerDirs.addAll(Arrays.asList(indexDirectories));
            }

            try (MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(
                URI.create(conf.getMetadataServiceUri())
            )) {
                driver.initialize(conf, () -> { }, NullStatsLogger.INSTANCE);
                Bookie.checkEnvironmentWithStorageExpansion(conf, driver,
                  Lists.newArrayList(journalDirectories), allLedgerDirs);
            } catch (BookieException e) {
                LOG.error("Exception occurred while updating cookie for storage expansion", e);
                response.setCode(HttpServer.StatusCode.INTERNAL_ERROR);
                response.setBody("Exception while updating cookie for storage expansion");
                return response;
            }

            String jsonResponse = "Success expand storage";
            LOG.debug("output body:" + jsonResponse);
            response.setBody(jsonResponse);
            response.setCode(HttpServer.StatusCode.OK);
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be PUT method");
            return response;
        }
    }
}
