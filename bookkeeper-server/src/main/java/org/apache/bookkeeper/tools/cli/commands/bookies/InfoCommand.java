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
package org.apache.bookkeeper.tools.cli.commands.bookies;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.cli.helpers.CommandHelpers;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;


/**
 * A bookie command to retrieve bookie info.
 */
public class InfoCommand extends BookieCommand<CliFlags> {

    private static final String NAME = "info";
    private static final String DESC = "Retrieve bookie info such as free and total disk space.";

    public InfoCommand() {
        super(CliSpec.newBuilder()
            .withName(NAME)
            .withFlags(new CliFlags())
            .withDescription(DESC)
            .build());
    }

    String getReadable(long val) {
        String[] unit = {"", "KB", "MB", "GB", "TB"};
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
    public boolean apply(ServerConfiguration conf, CliFlags cmdFlags) {

        ClientConfiguration clientConf = new ClientConfiguration(conf);
        clientConf.setDiskWeightBasedPlacementEnabled(true);
        try (BookKeeper bk = new BookKeeper(clientConf)) {
            Map<BookieSocketAddress, BookieInfo> map = bk.getBookieInfo();
            if (map.size() == 0) {
                System.out.println("Failed to retrieve bookie information from any of the bookies");
                bk.close();
                return true;
            }

            System.out.println("Free disk space info:");
            long totalFree = 0, total = 0;
            for (Map.Entry<BookieSocketAddress, BookieInfo> e : map.entrySet()) {
                BookieInfo bInfo = e.getValue();
                BookieSocketAddress bookieId = e.getKey();
                System.out.println(CommandHelpers.getBookieSocketAddrStringRepresentation(bookieId)
                    + ":\tFree: " + bInfo.getFreeDiskSpace() + getReadable(bInfo.getFreeDiskSpace())
                    + "\tTotal: " + bInfo.getTotalDiskSpace() + getReadable(bInfo.getTotalDiskSpace()));
            }

            // group by hostname
            Map<String, BookieInfo> dedupedMap = map.entrySet()
                .stream()
                .collect(Collectors.toMap(
                    entry -> entry.getKey().getHostName(),
                    entry -> entry.getValue(),
                    (key1, key2) -> key2
                ));
            for (BookieInfo bookieInfo : dedupedMap.values()) {
                totalFree += bookieInfo.getFreeDiskSpace();
                total += bookieInfo.getTotalDiskSpace();
            }

            System.out.println("Total free disk space in the cluster:\t" + totalFree + getReadable(totalFree));
            System.out.println("Total disk capacity in the cluster:\t" + total + getReadable(total));
            bk.close();

            return true;
        } catch (IOException | InterruptedException | BKException e) {
            e.printStackTrace();
        }
        return true;
    }
}
