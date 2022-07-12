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
import java.util.LinkedList;
import java.util.List;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Command to Query current auto recovery status.
 */
public class QueryAutoRecoveryStatusCommand
        extends BookieCommand<QueryAutoRecoveryStatusCommand.QFlags> {
    static final Logger LOG = LoggerFactory.
            getLogger(QueryAutoRecoveryStatusCommand.class);
    private static final String NAME = "queryautorecoverystatus";
    private static final String DESC = "Query autorecovery status.";

    public QueryAutoRecoveryStatusCommand() {
        super(CliSpec.<QFlags>newBuilder()
                .withName(NAME)
                .withDescription(DESC)
                .withFlags(new QFlags())
                .build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, QFlags cmdFlags) {
        try {
            return handler(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    /**
     * Flags for list under replicated command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class QFlags extends CliFlags{
        @Parameter(names =  {"-v", "--verbose"}, description = "list recovering detailed ledger info")
        private Boolean verbose = false;
    }

    private static class LedgerRecoverInfo {
        Long ledgerId;
        String bookieId;
        LedgerRecoverInfo(Long ledgerId, String bookieId) {
            this.ledgerId = ledgerId;
            this.bookieId = bookieId;
        }
    }

    /*
    Print Message format is like this:

     CurrentRecoverLedgerInfo:
        LedgerId:   BookieId:     LedgerSize:(detail)
        LedgerId:   BookieId:     LedgerSize:(detail)
     */
    public boolean handler(ServerConfiguration conf, QFlags flag) throws Exception {
        runFunctionWithLedgerManagerFactory(conf, mFactory -> {
            LedgerUnderreplicationManager underreplicationManager;
            LedgerManager ledgerManager = mFactory.newLedgerManager();
            List<LedgerRecoverInfo> ledgerList = new LinkedList<>();
            try {
                underreplicationManager = mFactory.newLedgerUnderreplicationManager();
            } catch (ReplicationException e) {
                throw new UncheckedExecutionException("Failed to new ledger underreplicated manager", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedExecutionException("Interrupted on newing ledger underreplicated manager", e);
            }
            Iterator<UnderreplicatedLedger> iter = underreplicationManager.listLedgersToRereplicate(null);
            while (iter.hasNext()) {
                UnderreplicatedLedger underreplicatedLedger = iter.next();
                long urLedgerId = underreplicatedLedger.getLedgerId();
                try {
                    String replicationWorkerId = underreplicationManager
                            .getReplicationWorkerIdRereplicatingLedger(urLedgerId);
                    if (replicationWorkerId != null) {
                        ledgerList.add(new LedgerRecoverInfo(urLedgerId, replicationWorkerId));
                    }
                } catch (ReplicationException.UnavailableException e) {
                    LOG.error("Failed to get ReplicationWorkerId rereplicating ledger {} -- {}", urLedgerId,
                            e.getMessage());
                }
            }

            LOG.info("CurrentRecoverLedgerInfo:");
            if (!flag.verbose) {
                for (int i = 0; i < ledgerList.size(); i++) {
                    LOG.info("\tLedgerId:{}\tBookieId:{}", ledgerList.get(i).ledgerId, ledgerList.get(i).bookieId);
                }
            } else {
                for (int i = 0; i < ledgerList.size(); i++) {
                    LedgerRecoverInfo info = ledgerList.get(i);
                    ledgerManager.readLedgerMetadata(info.ledgerId).whenComplete((metadata, exception) -> {
                        if (exception == null) {
                            LOG.info("\tLedgerId:{}\tBookieId:{}\tLedgerSize:{}",
                                    info.ledgerId, info.bookieId, metadata.getValue().getLength());
                        } else {
                            LOG.error("Unable to read the ledger: {} information", info.ledgerId);
                            throw new UncheckedExecutionException(exception);
                        }
                    });
                }
            }
            if (ledgerList.size() == 0) {
                // NO ledger is being auto recovering
                LOG.info("\t No Ledger is being recovered.");
            }
            return null;
        });
        return true;
    }
}
