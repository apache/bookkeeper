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
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.UpdateLedgerOp;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to update ledger command.
 */
public class FlipBookieIdCommand extends BookieCommand<FlipBookieIdCommand.FlipBookieIdFlags> {

    static final Logger LOG = LoggerFactory.getLogger(FlipBookieIdCommand.class);

    private static final String NAME = "flip-bookie-id";
    private static final String DESC = "Update bookie id in ledgers (this may take a long time).";

    public FlipBookieIdCommand() {
        this(new FlipBookieIdFlags());
    }

    private FlipBookieIdCommand(FlipBookieIdFlags flags) {
        super(CliSpec.<FlipBookieIdFlags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    /**
     * Flags for update ledger command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class FlipBookieIdFlags extends CliFlags {

        @Parameter(names = { "-host", "--hostname" },
            description = "Expects configuration useHostNameAsBookieID=true as the option value (default: ip address)")
        private boolean hostname;

        @Parameter(names = { "-s", "--updatepersec" },
            description = "Number of ledgers updating per second (default: 5 per sec)")
        private int updatePerSec = 5;

        @Parameter(names = { "-r",
                "--maxOutstandingReads" }, description = "Max outstanding reads (default: 5 * updatespersec)")
        private int maxOutstandingReads = updatePerSec * 5;

        @Parameter(names = {"-l", "--limit"},
            description = "Maximum number of ledgers of ledgers to update (default: no limit)")
        private int limit = Integer.MIN_VALUE;

        @Parameter(names = { "-v", "--verbose" }, description = "Print status of the ledger updation (default: false)")
        private boolean verbose;

        @Parameter(names = { "-p", "--printprogress" },
            description = "Print messages on every configured seconds if verbose turned on (default: 10 secs)")
        private long printProgress = 10;
    }

    @Override
    public boolean apply(ServerConfiguration conf, FlipBookieIdFlags cmdFlags) {
        try {
            return updateLedger(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean updateLedger(ServerConfiguration conf, FlipBookieIdFlags flags)
        throws InterruptedException, BKException, IOException {

        if (!conf.getUseHostNameAsBookieID() && flags.hostname) {
            LOG.error("Expects configuration useHostNameAsBookieID=true as the option value");
            return false;
        } else if (conf.getUseHostNameAsBookieID() && !flags.hostname) {
            LOG.error("Expects configuration useHostNameAsBookieID=false as the option value'");
            return false;
        }

        final int rate = flags.updatePerSec;
        if (rate <= 0) {
            LOG.error("Invalid updatespersec {}, should be > 0", rate);
            return false;
        }

        final int maxOutstandingReads = flags.maxOutstandingReads;
        if (maxOutstandingReads <= 0) {
            LOG.error("Invalid maxOutstandingReads {}, should be > 0", maxOutstandingReads);
            return false;
        }

        final int limit = flags.limit;
        if (limit <= 0 && limit != Integer.MIN_VALUE) {
            LOG.error("Invalid limit {}, should be > 0", limit);
            return false;
        }

        final long printProgress;
        if (flags.verbose) {
            printProgress = 10;
        } else {
            printProgress = flags.printProgress;
        }

        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.addConfiguration(conf);
        final BookKeeper bk = new BookKeeper(clientConfiguration);
        final BookKeeperAdmin admin = new BookKeeperAdmin(bk);
        final UpdateLedgerOp updateLedgerOp = new UpdateLedgerOp(bk, admin);
        final ServerConfiguration serverConfiguration = new ServerConfiguration(conf);
        final BookieSocketAddress newBookieId = Bookie.getBookieAddress(serverConfiguration);
        serverConfiguration.setUseHostNameAsBookieID(!flags.hostname);
        final BookieSocketAddress oldBookieId = Bookie.getBookieAddress(serverConfiguration);

        BookieShell.UpdateLedgerNotifier progressable = new BookieShell.UpdateLedgerNotifier() {
            long lastReport = System.nanoTime();

            @Override
            public void progress(long updated, long issued) {
                if (printProgress <= 0) {
                    return; // disabled
                }
                if (TimeUnit.MILLISECONDS.toSeconds(MathUtils.elapsedMSec(lastReport)) >= printProgress) {
                    LOG.info("Number of ledgers issued={}, updated={}", issued, updated);
                    lastReport = MathUtils.nowInNano();
                }
            }
        };

        try {
            updateLedgerOp.updateBookieIdInLedgers(oldBookieId, newBookieId, rate, maxOutstandingReads, limit,
                    progressable);
        } catch (IOException e) {
            LOG.error("Failed to update ledger metadata", e);
            return false;
        }

        return true;
    }
}
