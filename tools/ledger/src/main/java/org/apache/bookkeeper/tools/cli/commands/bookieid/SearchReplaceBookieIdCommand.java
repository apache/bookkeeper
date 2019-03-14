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
package org.apache.bookkeeper.tools.cli.commands.bookieid;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.RateLimiter;

import java.util.ArrayList;
import java.util.List;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.helpers.ClientCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Search and replace a bookie id in ledger metadata.
 */
public class SearchReplaceBookieIdCommand extends ClientCommand<SearchReplaceBookieIdCommand.Flags> {

    private static final String NAME = "searchreplace";
    private static final String DESC = "Search all ledgers for a bookie ID and replace";

    /**
     * Flags for replace bookie id.
     */
    @Accessors(fluent = true)
    @Setter
    public static class Flags extends CliFlags {

        @Parameter(names = { "-f", "--from" }, description = "Bookie ID to search for", required = true)
        private String from;
        @Parameter(names = { "-t", "--to" }, description = "Bookie ID to replace with", required = true)
        private String to;
        @Parameter(names = { "-m", "--max" }, description = "Maximum number of replacements to make")
        private long max = Long.MAX_VALUE;
        @Parameter(names = { "-r", "--rate" }, description = "Rate limit (updates per second)")
        private int rate = Integer.MAX_VALUE;
        @Parameter(names = { "--dry-run" }, description = "Don't actually write anything")
        private boolean dryRun = false;
        @Parameter(names = { "-v", "--verbose" }, description = "Verbose output")
        private boolean verbose = false;
    }

    public SearchReplaceBookieIdCommand() {
        this(new Flags());
    }

    public SearchReplaceBookieIdCommand(Flags flags) {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(flags)
            .build());
    }

    @Override
    protected void run(BookKeeper bk, Flags flags) throws Exception {
        try (BookKeeperAdmin admin = new BookKeeperAdmin((org.apache.bookkeeper.client.BookKeeper) bk)) {
            LedgerManager ledgerManager = ((org.apache.bookkeeper.client.BookKeeper) bk).getLedgerManager();
            long i = 0;

            BookieSocketAddress fromAddr = new BookieSocketAddress(flags.from);
            BookieSocketAddress toAddr = new BookieSocketAddress(flags.to);
            System.out.println(String.format("Replacing bookie id %s with %s in metadata", fromAddr, toAddr));
            RateLimiter limiter = RateLimiter.create(flags.rate);
            for (Long lid : admin.listLedgers()) {
                Versioned<LedgerMetadata> md = ledgerManager.readLedgerMetadata(lid).get();
                if (md.getValue().getAllEnsembles().entrySet()
                        .stream().anyMatch(e -> e.getValue().contains(fromAddr))) {
                    limiter.acquire();

                    LedgerMetadataBuilder builder = LedgerMetadataBuilder.from(md.getValue());
                    md.getValue().getAllEnsembles().entrySet().stream()
                        .filter(e -> e.getValue().contains(fromAddr))
                        .forEach(e -> {
                                List<BookieSocketAddress> ensemble = new ArrayList<>(e.getValue());
                                ensemble.replaceAll((a) -> {
                                        if (a.equals(fromAddr)) {
                                            return toAddr;
                                        } else {
                                            return a;
                                        }
                                    });
                                builder.replaceEnsembleEntry(e.getKey(), ensemble);
                            });
                    LedgerMetadata newMeta = builder.build();
                    if (flags.verbose) {
                        System.out.println("Replacing ledger " + lid + " metadata ...");
                        System.out.println(md.getValue().toSafeString());
                        System.out.println("with ...");
                        System.out.println(newMeta.toSafeString());
                    }
                    i++;
                    if (!flags.dryRun) {
                        ledgerManager.writeLedgerMetadata(lid, newMeta, md.getVersion()).get();
                    }
                }
                if (i >= flags.max) {
                    System.out.println("Max number of ledgers processed, exiting");
                    break;
                }
            }
            System.out.println("Replaced bookie ID in " + i + " ledgers");
        }
    }
}
