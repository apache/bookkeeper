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
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.storage.ldb.LedgersIndexRebuildOp;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to rebuild DBLedgerStorage ledgers index.
 */
public class RebuildDBLedgersIndexCommand extends BookieCommand<RebuildDBLedgersIndexCommand.RebuildLedgersIndexFlags> {

    static final Logger LOG = LoggerFactory.getLogger(RebuildDBLedgersIndexCommand.class);

    private static final String NAME = "rebuild-db-ledgers-index";
    private static final String DESC = "Rebuild DBLedgerStorage ledgers index by scanning the journal"
        + " and entry logs (sets all ledgers to fenced)";

    public RebuildDBLedgersIndexCommand() {
        this(new RebuildLedgersIndexFlags());
    }

    public RebuildDBLedgersIndexCommand(RebuildLedgersIndexFlags flags) {
        super(CliSpec.<RebuildDBLedgersIndexCommand.RebuildLedgersIndexFlags>newBuilder().withName(NAME)
                .withDescription(DESC).withFlags(flags).build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, RebuildLedgersIndexFlags cmdFlags) {
        LOG.info("=== Rebuilding DBStorage ledgers index ===");
        ServerConfiguration serverConfiguration = new ServerConfiguration(conf);
        boolean success = new LedgersIndexRebuildOp(serverConfiguration, cmdFlags.verbose).initiate();
        if (success) {
            LOG.info("-- Done rebuilding DBStorage ledgers index --");
        } else {
            LOG.info("-- Aborted rebuilding DBStorage ledgers index --");
        }

        return success;
    }

    /**
     * Flags for read log command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class RebuildLedgersIndexFlags extends CliFlags {
        @Parameter(names = { "-v", "--verbose" },
                description = "Verbose logging. Print each ledger id found and added to the rebuilt index")
        private boolean verbose;
    }
}
