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

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.http.HttpRouter;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.meta.MigrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.HttpClientUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migrate the data of the specified replica to other bookie nodes.
 * */
public class MigrationReplicasCommand extends BookieCommand<MigrationReplicasCommand.MigrationReplicasFlags> {
    static final Logger LOG = LoggerFactory.getLogger(MigrationReplicasCommand.class);
    private static final String NAME = "migrationReplicas";
    private static final String DESC = "Migrate the data of the specified replica to other bookie nodes";
    public MigrationReplicasCommand() {
        this(new MigrationReplicasFlags());
    }
    private MigrationReplicasCommand(MigrationReplicasFlags flags) {
        super(CliSpec.<MigrationReplicasFlags>newBuilder()
                .withName(NAME)
                .withDescription(DESC)
                .withFlags(flags)
                .build());
    }

    /**
     * Flags for migrationReplicas command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class MigrationReplicasFlags extends CliFlags {

        @Parameter(names = { "-b", "--bookieIds" }, description = "Bookies corresponding to the migrated replica, "
                + "eg: bookieId1,bookieId2,bookieId3")
        private List<String> bookieIdsOfMigratedReplica;

        @Parameter(names = { "-l", "--ledgerIds" }, description = "The ledgerIds corresponding to the migrated replica,"
                + "eg: ledgerId1,ledgerId2,ledgerId3. The default is empty, which means that all replica data on these "
                + "bookie nodes will be migrated to other bookie nodes")
        private List<Long> ledgerIdsOfMigratedReplica = Collections.emptyList();

        @Parameter(names = { "-r", "--readOnly" }, description = "Whether to set the bookie nodes of the migrated data"
                + " to readOnly, the default is false")
        private boolean readOnly = false;
    }

    @Override
    public boolean apply(ServerConfiguration conf, MigrationReplicasFlags cmdFlags) {
        try (BookKeeperAdmin admin = new BookKeeperAdmin(new ClientConfiguration(conf))) {
            if (cmdFlags.readOnly) {
                if (!conf.isHttpServerEnabled()) {
                    throw new RuntimeException("Please enable http service first, config httpServerEnabled is false!");
                }
                Set<BookieId> toSwitchBookieIdsSet = new HashSet<>();
                for (String bookieIdStr : cmdFlags.bookieIdsOfMigratedReplica) {
                    toSwitchBookieIdsSet.add(BookieId.parse(bookieIdStr));
                }

                Collection<BookieId> readOnlyBookies = admin.getReadOnlyBookies();
                toSwitchBookieIdsSet.removeAll(readOnlyBookies);
                Set<BookieId> switchedBookies = new HashSet<>();
                if (!switchToReadonly(admin, toSwitchBookieIdsSet, switchedBookies, true)) {
                    LOG.warn("Some bookie nodes that fail to set readonly, "
                            + "and the successful bookies fall back to the previous state!");
                    Set<BookieId> fallbackSuccessfulBookies = new HashSet<>();
                    if (!switchToReadonly(admin, switchedBookies, fallbackSuccessfulBookies, false)) {
                        switchedBookies.removeAll(fallbackSuccessfulBookies);
                        LOG.error(String.format("Fallback failed! Failed nodes:%s", switchedBookies));
                    }
                    return false;
                }
            }

            return migrationReplicas(admin, conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean switchToReadonly(BookKeeperAdmin admin,
                                     Set<BookieId> toSwitchBookieIdsSet,
                                     Set<BookieId> switchedBookieIds,
                                     boolean toReadOnly) throws BKException {
        for (BookieId bookieId : toSwitchBookieIdsSet) {
            BookieServiceInfo bookieServiceInfo = admin.getBookieServiceInfo(bookieId);
            for (BookieServiceInfo.Endpoint endpoint : bookieServiceInfo.getEndpoints()) {
                if (endpoint.getProtocol().equals("http")) {
                    String host = endpoint.getHost();
                    int port = endpoint.getPort();
                    String url = String.format("http://%s:%s/%s", host, port, HttpRouter.BOOKIE_STATE_READONLY);
                    String readOnly = toReadOnly ? "true" : "false";
                    CloseableHttpResponse response = HttpClientUtils.doPutRequest(url, readOnly, "UTF-8");
                    int responseCode = response.getStatusLine().getStatusCode();
                    if (responseCode != HttpServer.StatusCode.OK.getValue()) {
                        LOG.error(String.format("Set readonly to %s failed! bookieId:%s, responseCode:%s ",
                                readOnly, bookieId, responseCode));
                        return false;
                    }

                    switchedBookieIds.add(bookieId);
                    LOG.info(String.format("Set readonly to %s success! bookieId:%s, responseCode:%s",
                            readOnly, bookieId, responseCode));
                }
            }
        }
        return true;
    }

    private boolean migrationReplicas(BookKeeperAdmin admin, ServerConfiguration conf, MigrationReplicasFlags flags)
            throws BKException, InterruptedException {
        try {
            // 1. Build bookieIdsSet adn ledgerIdsToMigrate
            Set<BookieId> bookieIdsSet = new HashSet<>();
            for (String bookieIdStr : flags.bookieIdsOfMigratedReplica) {
                bookieIdsSet.add(BookieId.parse(bookieIdStr));
            }
            HashSet<Long> ledgerIdsToMigrate = new HashSet<>();
            for (Long ledgerId : flags.ledgerIdsOfMigratedReplica) {
                ledgerIdsToMigrate.add(ledgerId);
            }

            // 2. Build toMigratedLedgerAndBookieMap
            Map<Long, Set<BookieId>> toMigratedLedgerAndBookieMap = new HashMap<>();
            Map<BookieId, Set<Long>> bookieLedgerMaps = admin.listLedgers(bookieIdsSet);
            for (Map.Entry<BookieId, Set<Long>> bookieIdSetEntry : bookieLedgerMaps.entrySet()) {
                Set<Long> ledgersOnBookieId = bookieIdSetEntry.getValue();
                if (!ledgerIdsToMigrate.isEmpty()) {
                    ledgersOnBookieId.retainAll(ledgerIdsToMigrate);
                }
                for (Long ledgerId : ledgersOnBookieId) {
                    Set<BookieId> bookieIdSet = toMigratedLedgerAndBookieMap.getOrDefault(ledgerId, new HashSet<>());
                    bookieIdSet.add(bookieIdSetEntry.getKey());
                    toMigratedLedgerAndBookieMap.put(ledgerId, bookieIdSet);
                }
            }

            // 3. Submit ledger replicas to be migrated
            MigrationManager migrationManager = admin.getMigrationManager();
            migrationManager.submitToMigrateReplicas(toMigratedLedgerAndBookieMap);
            return true;
        } catch (Exception e) {
            LOG.error("Received exception in MigrationReplicasCommand ", e);
            return false;
        } finally {
            admin.close();
        }
    }
}
