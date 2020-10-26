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
package org.apache.bookkeeper.tools.cli.commands.client;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LedgerIdFormatter;

/**
 * Command to delete a given ledger.
 */
public class DeleteLedgerCommand extends BookieCommand<DeleteLedgerCommand.DeleteLedgerFlags> {

    private static final String NAME = "delete";
    private static final String DESC = "Delete a ledger.";
    private static final String DEFAULT = "";

    private LedgerIdFormatter ledgerIdFormatter;

    public DeleteLedgerCommand() {
        this(new DeleteLedgerFlags());
    }

    public DeleteLedgerCommand(LedgerIdFormatter ledgerIdFormatter) {
        this(new DeleteLedgerFlags());
        this.ledgerIdFormatter = ledgerIdFormatter;
    }

    private DeleteLedgerCommand(DeleteLedgerFlags flags) {
        super(CliSpec.<DeleteLedgerCommand.DeleteLedgerFlags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    /**
     * Flags for delete ledger command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class DeleteLedgerFlags extends CliFlags {

        @Parameter(names = { "-l", "--ledgerid" }, description = "Ledger ID", required = true)
        private long ledgerId;

        @Parameter(names = { "-f", "--force" }, description = "Whether to force delete the Ledger without prompt..?")
        private boolean force;

        @Parameter(names = { "-lf", "--ledgeridformatter" }, description = "Set ledger id formatter")
        private String ledgerIdFormatter = DEFAULT;

    }

    @Override
    public boolean apply(ServerConfiguration conf, DeleteLedgerFlags cmdFlags) {
        initLedgerIdFormatter(conf, cmdFlags);
        try {
            return deleteLedger(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private void initLedgerIdFormatter(ServerConfiguration conf, DeleteLedgerFlags flags) {
        if (null == ledgerIdFormatter && !flags.ledgerIdFormatter.equals(DEFAULT)) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(flags.ledgerIdFormatter, conf);
        } else if (null == ledgerIdFormatter && flags.ledgerIdFormatter.equals(DEFAULT)) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(conf);
        }
    }

    private boolean deleteLedger(ServerConfiguration conf, DeleteLedgerFlags flags)
        throws IOException, BKException, InterruptedException {

        if (flags.ledgerId < 0) {
            System.err.println("Ledger id error.");
            return false;
        }

        boolean confirm = false;
        if (!flags.force) {
            confirm = IOUtils.confirmPrompt(
                "Are your sure to delete Ledger : " + ledgerIdFormatter.formatLedgerId(flags.ledgerId) + "?");
        }

        BookKeeper bookKeeper = null;
        try {
            if (flags.force || confirm) {
                ClientConfiguration configuration = new ClientConfiguration();
                configuration.addConfiguration(conf);
                bookKeeper = new BookKeeper(configuration);
                bookKeeper.deleteLedger(flags.ledgerId);
            }
        } finally {
            if (bookKeeper != null) {
                bookKeeper.close();
            }
        }

        return true;
    }
}
