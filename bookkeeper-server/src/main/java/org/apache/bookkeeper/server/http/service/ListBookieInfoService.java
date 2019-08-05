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

import com.google.common.collect.Maps;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookieInfoReader;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService that handle Bookkeeper list bookie info related http request.
 *
 * <p>The GET method will get the disk usage of all bookies in this bookkeeper cluster.
 * Output would be like this:
 *  {
 *    "bookieAddress" : {free: xxx, total: xxx}",
 *    "bookieAddress" : {free: xxx, total: xxx},
 *    ...
 *    "clusterInfo" : {total_free: xxx, total: xxx}"
 *  }
 */
public class ListBookieInfoService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(ListBookieInfoService.class);

    protected ServerConfiguration conf;

    public ListBookieInfoService(ServerConfiguration conf) {
        checkNotNull(conf);
        this.conf = conf;
    }

    String getReadable(long val) {
        String[] unit = {"", "KB", "MB", "GB", "TB" };
        int cnt = 0;
        double d = val;
        while (d >= 1000 && cnt < unit.length - 1) {
            d = d / 1000;
            cnt++;
        }
        DecimalFormat df = new DecimalFormat("#.###");
        df.setRoundingMode(RoundingMode.DOWN);
        return cnt > 0 ? "(" + df.format(d) + unit[cnt] + ")" : unit[cnt];
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.GET == request.getMethod()) {
            ClientConfiguration clientConf = new ClientConfiguration(conf);
            clientConf.setDiskWeightBasedPlacementEnabled(true);
            BookKeeper bk = new BookKeeper(clientConf);

            Map<BookieSocketAddress, BookieInfoReader.BookieInfo> map = bk.getBookieInfo();
            if (map.size() == 0) {
                bk.close();
                response.setCode(HttpServer.StatusCode.NOT_FOUND);
                response.setBody("Not found any Bookie info.");
                return response;
            }

            /**
             * output:
             *  {
             *    "bookieAddress" : {free: xxx, total: xxx}",
             *    "bookieAddress" : {free: xxx, total: xxx},
             *    ...
             *    "clusterInfo" : {total_free: xxx, total: xxx}"
             *  }
             */
            LinkedHashMap<String, String> output = Maps.newLinkedHashMapWithExpectedSize(map.size());
            Long totalFree = 0L, total = 0L;
            for (Map.Entry<BookieSocketAddress, BookieInfoReader.BookieInfo> infoEntry : map.entrySet()) {
                BookieInfoReader.BookieInfo bInfo = infoEntry.getValue();
                output.put(infoEntry.getKey().toString(),
                    ": {Free: " + bInfo.getFreeDiskSpace() + getReadable(bInfo.getFreeDiskSpace())
                    + ", Total: " + bInfo.getTotalDiskSpace() + getReadable(bInfo.getTotalDiskSpace()) + "},");
                totalFree += bInfo.getFreeDiskSpace();
                total += bInfo.getTotalDiskSpace();
            }
            output.put("ClusterInfo: ",
                "{Free: " + totalFree + getReadable(totalFree)
                + ", Total: " + total + getReadable(total) + "}");

            bk.close();

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
