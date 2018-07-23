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
import com.google.common.collect.Maps;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LogMark;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.util.DiskChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper get last log mark related http request.
 * The GET method will get the last log position of each journal.
 *
 * <p>output would be like this:
 *  {
 *    "&lt;Journal_id&gt;" : "&lt;Pos&gt;",
 *    ...
 *  }
 */
public class GetLastLogMarkService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(GetLastLogMarkService.class);

    protected ServerConfiguration conf;

    public GetLastLogMarkService(ServerConfiguration conf) {
        checkNotNull(conf);
        this.conf = conf;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.GET == request.getMethod()) {
            try {
                /**
                 * output:
                 *  {
                 *    "&lt;Journal_id&gt;" : "&lt;Pos&gt;",
                 *    ...
                 *  }
                 */
                Map<String, String> output = Maps.newHashMap();

                List<Journal> journals = Lists.newArrayListWithCapacity(conf.getJournalDirs().length);
                int idx = 0;
                for (File journalDir : conf.getJournalDirs()) {
                    journals.add(new Journal(idx++, journalDir, conf, new LedgerDirsManager(conf, conf.getLedgerDirs(),
                      new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()))));
                }
                for (Journal journal : journals) {
                    LogMark lastLogMark = journal.getLastLogMark().getCurMark();
                    LOG.debug("LastLogMark: Journal Id - " + lastLogMark.getLogFileId() + "("
                      + Long.toHexString(lastLogMark.getLogFileId()) + ".txn), Pos - "
                      + lastLogMark.getLogFileOffset());
                    output.put("LastLogMark: Journal Id - " + lastLogMark.getLogFileId()
                        + "(" + Long.toHexString(lastLogMark.getLogFileId()) + ".txn)",
                        "Pos - " + lastLogMark.getLogFileOffset());
                }

                String jsonResponse = JsonUtil.toJson(output);
                LOG.debug("output body:" + jsonResponse);
                response.setBody(jsonResponse);
                response.setCode(HttpServer.StatusCode.OK);
                return response;
            } catch (Exception e) {
                LOG.error("Exception occurred while getting last log mark", e);
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("ERROR handling request: " + e.getMessage());
                return response;
            }
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be GET method");
            return response;
        }
    }
}
