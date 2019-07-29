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
import static org.apache.bookkeeper.bookie.BookieShell.listFilesAndSort;

import com.google.common.collect.Maps;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper list disk files related http request.
 *
 * <p>The GET method will list all bookie files of type journal|entrylog|index in this bookie.
 * The output would be like this:
 *  {
 *    "journal files" : "filename1 \t ...",
 *    "entrylog files" : "filename1 \t ...",
 *    "index files" : "filename1 \t ..."
 *  }
 */
public class ListDiskFilesService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(ListDiskFilesService.class);

    protected ServerConfiguration conf;

    public ListDiskFilesService(ServerConfiguration conf) {
        checkNotNull(conf);
        this.conf = conf;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();
        Map<String, String> params = request.getParams();

        if (HttpServer.Method.GET == request.getMethod()) {
            /**
             * output:
             *  {
             *    "journal files" : "filename \t ...",
             *    "entrylog files" : "filename \t ...",
             *    "index files" : "filename \t ..."
             *  }
             */
            Map<String, String> output = Maps.newHashMap();

            boolean journal = params != null
                && params.containsKey("file_type")
                && params.get("file_type").equals("journal");
            boolean entrylog = params != null
                && params.containsKey("file_type")
                && params.get("file_type").equals("entrylog");
            boolean index = params != null
                && params.containsKey("file_type")
                && params.get("file_type").equals("index");
            boolean all = false;

            if (!journal && !entrylog && !index && !all) {
                all = true;
            }

            if (all || journal) {
                File[] journalDirs = conf.getJournalDirs();
                List<File> journalFiles = listFilesAndSort(journalDirs, "txn");
                StringBuilder files = new StringBuilder();
                for (File journalFile : journalFiles) {
                    files.append(journalFile.getName()).append("\t");
                }
                output.put("journal files", files.toString());
            }

            if (all || entrylog) {
                File[] ledgerDirs = conf.getLedgerDirs();
                List<File> ledgerFiles = listFilesAndSort(ledgerDirs, "log");
                StringBuilder files = new StringBuilder();
                for (File ledgerFile : ledgerFiles) {
                    files.append(ledgerFile.getName()).append("\t");
                }
                output.put("entrylog files", files.toString());
            }

            if (all || index) {
                File[] indexDirs = (conf.getIndexDirs() == null) ? conf.getLedgerDirs() : conf.getIndexDirs();
                List<File> indexFiles = listFilesAndSort(indexDirs, "idx");
                StringBuilder files = new StringBuilder();
                for (File indexFile : indexFiles) {
                    files.append(indexFile.getName()).append("\t");
                }
                output.put("index files", files.toString());
            }

            String jsonResponse = JsonUtil.toJson(output);
            LOG.debug("output body:" + jsonResponse);
            response.setBody(jsonResponse);
            response.setCode(HttpServer.StatusCode.OK);
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be GET method");
            return response;
        }
    }
}
