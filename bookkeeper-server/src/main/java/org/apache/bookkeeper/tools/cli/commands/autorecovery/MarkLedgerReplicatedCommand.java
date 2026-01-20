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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarkLedgerReplicatedCommand extends BookieCommand<MarkLedgerReplicatedCommand.MarkLedgerReplicatedFlags> {

    static final Logger LOG = LoggerFactory.getLogger(MarkLedgerReplicatedCommand.class);
    private static final String NAME = "markledgerreplicated";
    private static final String DESC = "Mark a ledger replicated.";
    private static final String DEFAULT = "";
    private LedgerIdFormatter ledgerIdFormatter;

    public MarkLedgerReplicatedCommand() {
        this(new MarkLedgerReplicatedCommand.MarkLedgerReplicatedFlags());
    }

    public MarkLedgerReplicatedCommand(LedgerIdFormatter ledgerIdFormatter) {
        this(new MarkLedgerReplicatedCommand.MarkLedgerReplicatedFlags());
        this.ledgerIdFormatter = ledgerIdFormatter;
    }

    private MarkLedgerReplicatedCommand(MarkLedgerReplicatedCommand.MarkLedgerReplicatedFlags flags) {
        super(CliSpec.<MarkLedgerReplicatedCommand.MarkLedgerReplicatedFlags>newBuilder().withName(NAME)
                .withDescription(DESC).withFlags(flags).build());
    }

    /**
     * Flags for delete ledger command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class MarkLedgerReplicatedFlags extends CliFlags {

        @Parameter(names = {"-l", "--ledgerid"}, description = "Ledger ID", required = true)
        private long ledgerId;

        @Parameter(names = {"-f",
                "--force"}, description = "Whether to force mark the Ledger replicated without prompt..?")
        private boolean force;

        @Parameter(names = {"-lf", "--ledgeridformatter"}, description = "Set ledger id formatter")
        private String ledgerIdFormatter = DEFAULT;
    }

    @Override
    public boolean apply(ServerConfiguration conf, MarkLedgerReplicatedFlags cmdFlags) {
        initLedgerIdFormatter(conf, cmdFlags);
        try {
            return markLedgerReplicated(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private void initLedgerIdFormatter(ServerConfiguration conf,
            MarkLedgerReplicatedCommand.MarkLedgerReplicatedFlags flags) {
        if (null == ledgerIdFormatter && !flags.ledgerIdFormatter.equals(DEFAULT)) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(flags.ledgerIdFormatter, conf);
        } else if (null == ledgerIdFormatter && flags.ledgerIdFormatter.equals(DEFAULT)) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(conf);
        }
    }

    private boolean markLedgerReplicated(ServerConfiguration bkConf,
            MarkLedgerReplicatedCommand.MarkLedgerReplicatedFlags flags)
            throws ExecutionException, MetadataException {
        return runFunctionWithLedgerManagerFactory(bkConf, mFactory -> {
            LedgerUnderreplicationManager underreplicationManager;
            try {
                underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                if (flags.ledgerId < 0) {
                    LOG.error("Ledger id error.");
                    return false;
                }
                boolean confirm = false;
                if (!flags.force) {
                    confirm = IOUtils.confirmPrompt(
                            "Are your sure to mark Ledger:" + ledgerIdFormatter.formatLedgerId(flags.ledgerId)
                                    + " replicated?");
                }
                if (flags.force || confirm) {
                    underreplicationManager.markLedgerReplicated(flags.ledgerId);
                    return true;
                } else {
                    return false;
                }
            } catch (ReplicationException e) {
                throw new UncheckedExecutionException("Failed to new ledger underreplicated manager", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedExecutionException("Interrupted on newing ledger underreplicated manager", e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

}
