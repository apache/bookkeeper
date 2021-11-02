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
package org.apache.bookkeeper.tools.cli.commands.bookie;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.storage.ldb.LedgersIndexCheckOp;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to check the DBLedgerStorage ledgers index integrity.
 */
public class CheckDBLedgersIndexCommand extends BookieCommand<CheckDBLedgersIndexCommand.CheckLedgersIndexFlags> {

    static final Logger LOG = LoggerFactory.getLogger(CheckDBLedgersIndexCommand.class);

    private static final String NAME = "check-db-ledgers-index";
    private static final String DESC = "Check the DBLedgerStorage ledgers index integrity by performing a read scan";

    public CheckDBLedgersIndexCommand() {
        this(new CheckLedgersIndexFlags());
    }

    public CheckDBLedgersIndexCommand(CheckLedgersIndexFlags flags) {
        super(CliSpec.<CheckLedgersIndexFlags>newBuilder().withName(NAME)
                .withDescription(DESC).withFlags(flags).build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, CheckLedgersIndexFlags cmdFlags) {
        LOG.info("=== Checking DBStorage ledgers index by running a read scan ===");
        ServerConfiguration serverConfiguration = new ServerConfiguration(conf);
        try {
            boolean success = new LedgersIndexCheckOp(serverConfiguration, cmdFlags.verbose).initiate();
            if (success) {
                LOG.info("-- Done checking DBStorage ledgers index --");
            } else {
                LOG.info("-- Aborted checking DBStorage ledgers index --");
            }

            return success;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

    /**
     * Flags for read log command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class CheckLedgersIndexFlags extends CliFlags {
        @Parameter(names = { "-v", "--verbose" }, description = "Verbose logging. Print each ledger.")
        private boolean verbose;
    }
}
