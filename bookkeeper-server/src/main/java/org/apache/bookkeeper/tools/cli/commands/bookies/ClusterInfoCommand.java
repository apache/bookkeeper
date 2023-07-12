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

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithLedgerManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.Iterator;
import lombok.Data;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bookie command to retrieve bookies cluster info.
 */
public class ClusterInfoCommand extends BookieCommand<CliFlags> {

    private static final String NAME = "cluster-info";
    private static final String DESC = "Exposes the current info about the cluster of bookies";
    private static final Logger LOG = LoggerFactory.getLogger(ClusterInfoCommand.class);
    private ClusterInfo info;

    public ClusterInfoCommand() {
        super(CliSpec.newBuilder()
                .withName(NAME)
                .withFlags(new CliFlags())
                .withDescription(DESC)
                .build());
    }

    /**
     * POJO definition for the cluster info response.
     */
    @Data
    public static class ClusterInfo {
        private boolean auditorElected;
        private String auditorId;
        private boolean clusterUnderReplicated;
        private boolean ledgerReplicationEnabled;
        private int totalBookiesCount;
        private int writableBookiesCount;
        private int readonlyBookiesCount;
        private int unavailableBookiesCount;
    }

    @Override
    public boolean apply(ServerConfiguration conf, CliFlags cmdFlags) {

        ClientConfiguration clientConfiguration = new ClientConfiguration(conf);
        try (BookKeeperAdmin admin = new BookKeeperAdmin(clientConfiguration)) {
            LOG.info("Starting fill cluster info.");
            info = new ClusterInfo();
            fillUReplicatedInfo(info, conf);
            fillAuditorInfo(info, admin);
            fillBookiesInfo(info, admin);

            LOG.info("--------- Cluster Info ---------");
            LOG.info("{}", JsonUtil.toJson(info));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    private void fillBookiesInfo(ClusterInfo info, BookKeeperAdmin bka) throws BKException {
        int totalBookiesCount = bka.getAllBookies().size();
        int writableBookiesCount = bka.getAvailableBookies().size();
        int readonlyBookiesCount = bka.getReadOnlyBookies().size();
        int unavailableBookiesCount = totalBookiesCount - writableBookiesCount - readonlyBookiesCount;

        info.setTotalBookiesCount(totalBookiesCount);
        info.setWritableBookiesCount(writableBookiesCount);
        info.setReadonlyBookiesCount(readonlyBookiesCount);
        info.setUnavailableBookiesCount(unavailableBookiesCount);
    }

    private void fillAuditorInfo(ClusterInfo info, BookKeeperAdmin bka) {
        try {
            BookieId currentAuditor = bka.getCurrentAuditor();
            info.setAuditorElected(currentAuditor != null);
            info.setAuditorId(currentAuditor == null ? "" : currentAuditor.getId());
        } catch (Exception e) {
            LOG.error("Could not get Auditor info", e);
            info.setAuditorElected(false);
            info.setAuditorId("");
        }
    }

    private void fillUReplicatedInfo(ClusterInfo info, ServerConfiguration conf) throws Exception {
        runFunctionWithLedgerManagerFactory(conf, mFactory -> {
            try (LedgerUnderreplicationManager underreplicationManager =
                         mFactory.newLedgerUnderreplicationManager()) {
                Iterator<UnderreplicatedLedger> iter = underreplicationManager.listLedgersToRereplicate(null);

                info.setClusterUnderReplicated(iter.hasNext());
                info.setLedgerReplicationEnabled(underreplicationManager.isLedgerReplicationEnabled());
            } catch (Exception e) {
                throw new UncheckedExecutionException(e);
            }
            return null;
        });
    }

    @VisibleForTesting
    public ClusterInfo info() {
        return info;
    }
}
