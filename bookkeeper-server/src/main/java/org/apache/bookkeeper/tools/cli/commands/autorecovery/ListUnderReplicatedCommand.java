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
package org.apache.bookkeeper.tools.cli.commands.autorecovery;

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithLedgerManagerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to listing under replicated ledgers.
 */
public class ListUnderReplicatedCommand extends BookieCommand<ListUnderReplicatedCommand.LURFlags> {

    static final Logger LOG = LoggerFactory.getLogger(ListUnderReplicatedCommand.class);

    private static final String NAME = "listunderreplicated";
    private static final String DESC = "List ledgers marked as underreplicated, with oprional options to specify "
                                       + "missingreplica (BookieId) and to exclude missingreplica.";
    private static final String DEFAULT = "";

    private LedgerIdFormatter ledgerIdFormatter;

    public ListUnderReplicatedCommand() {
        this(new LURFlags());
    }

    public ListUnderReplicatedCommand(LedgerIdFormatter ledgerIdFormatter) {
        this();
        this.ledgerIdFormatter = ledgerIdFormatter;
    }

    private ListUnderReplicatedCommand(LURFlags flags) {
        super(CliSpec.<LURFlags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    /**
     * Flags for list under replicated command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class LURFlags extends CliFlags{

        @Parameter(names = { "-pmr", "--printmissingreplica" }, description = "Whether to print missingreplicas list?")
        private boolean printMissingReplica;

        @Parameter(names = { "-prw",
            "--printreplicationworkerid" }, description = "Whether wo print replicationworkerid?")
        private boolean printReplicationWorkerId;

        @Parameter(names = { "-mr", "--missingreplica" }, description = "Bookie Id of missing replica")
        private String missingReplica = DEFAULT;

        @Parameter(names = { "-emr", "--excludingmissingreplica" }, description = "Bookie Id of missing replica to "
                                                                                  + "ignore")
        private String excludingMissingReplica = DEFAULT;

        @Parameter(names =  {"-l", "--ledgeridformatter"}, description = "Set ledger id formatter")
        private String ledgerIdFormatter = DEFAULT;
    }

    @Override
    public boolean apply(ServerConfiguration conf, LURFlags cmdFlags) {
        if (!cmdFlags.ledgerIdFormatter.equals(DEFAULT) && ledgerIdFormatter == null) {
            ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(cmdFlags.ledgerIdFormatter, conf);
        } else if (ledgerIdFormatter == null) {
            ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(conf);
        }
        try {
            return handler(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    public boolean handler(ServerConfiguration bkConf, LURFlags flags) throws MetadataException, ExecutionException {
        final String includingBookieId = flags.missingReplica;
        final String excludingBookieId = flags.excludingMissingReplica;
        final boolean printMissingReplica = flags.printMissingReplica;
        final boolean printReplicationWorkerId = flags.printReplicationWorkerId;

        final Predicate<List<String>> predicate;
        if (!StringUtils.isBlank(includingBookieId) && !StringUtils.isBlank(excludingBookieId)) {
            predicate = replicasList -> (replicasList.contains(includingBookieId)
                                         && !replicasList.contains(excludingBookieId));
        } else if (!StringUtils.isBlank(includingBookieId)) {
            predicate = replicasList -> replicasList.contains(includingBookieId);
        } else if (!StringUtils.isBlank(excludingBookieId)) {
            predicate = replicasList -> !replicasList.contains(excludingBookieId);
        } else {
            predicate = null;
        }

        runFunctionWithLedgerManagerFactory(bkConf, mFactory -> {
            LedgerUnderreplicationManager underreplicationManager;
            try {
                underreplicationManager = mFactory.newLedgerUnderreplicationManager();
            } catch (KeeperException | ReplicationException.CompatibilityException e) {
                throw new UncheckedExecutionException("Failed to new ledger underreplicated manager", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedExecutionException("Interrupted on newing ledger underreplicated manager", e);
            }
            Iterator<UnderreplicatedLedger> iter = underreplicationManager.listLedgersToRereplicate(predicate);
            while (iter.hasNext()) {
                UnderreplicatedLedger underreplicatedLedger = iter.next();
                long urLedgerId = underreplicatedLedger.getLedgerId();
                System.out.println(ledgerIdFormatter.formatLedgerId(urLedgerId));
                long ctime = underreplicatedLedger.getCtime();
                if (ctime != UnderreplicatedLedger.UNASSIGNED_CTIME) {
                    System.out.println("\tCtime : " + ctime);
                }
                if (printMissingReplica) {
                    underreplicatedLedger.getReplicaList().forEach((missingReplica) -> {
                        System.out.println("\tMissingReplica : " + missingReplica);
                    });
                }
                if (printReplicationWorkerId) {
                    try {
                        String replicationWorkerId = underreplicationManager
                                                         .getReplicationWorkerIdRereplicatingLedger(urLedgerId);
                        if (replicationWorkerId != null) {
                            System.out.println("\tReplicationWorkerId : " + replicationWorkerId);
                        }
                    } catch (ReplicationException.UnavailableException e) {
                        LOG.error("Failed to get ReplicationWorkerId rereplicating ledger {} -- {}", urLedgerId,
                                  e.getMessage());
                    }
                }
            }
            return null;
        });
        return true;
    }

}

