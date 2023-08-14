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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.LocalBookieEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.commands.bookie.SanityTestCommand.SanityFlags;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bookie command to sanity test for local bookie.
 */
public class SanityTestCommand extends BookieCommand<SanityFlags> {

    private static final Logger LOG = LoggerFactory.getLogger(SanityTestCommand.class);
    private static final String NAME = "sanitytest";
    private static final String DESC = "Sanity test for local bookie. "
                                           + "Create ledger and write/reads entries on local bookie.";

    public SanityTestCommand() {
        this(new SanityFlags());
    }

    public SanityTestCommand(SanityFlags flags) {
        super(CliSpec.<SanityFlags>newBuilder().withFlags(flags).withName(NAME).withDescription(DESC).build());
    }

    /**
     * Flags for sanity command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class SanityFlags extends CliFlags{

        @Parameter(names = {"-e", "--entries"}, description = "Total entries to be added for the test (default 10)")
        private int entries = 10;

        @Parameter(names = { "-t",
            "--timeout" }, description = "Timeout for write/read operations in seconds (default 1)")
        private int timeout = 1;

    }

    @Override
    public boolean apply(ServerConfiguration conf, SanityFlags cmdFlags) {
        try {
            return handle(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private static boolean handle(ServerConfiguration conf, SanityFlags cmdFlags) throws Exception {
        try {
            return handleAsync(conf, cmdFlags).get();
        } catch (Exception e) {
            LOG.warn("Error in bookie sanity test", e);
            return false;
        }
    }

    public static CompletableFuture<Boolean> handleAsync(ServerConfiguration conf, SanityFlags cmdFlags) {
        CompletableFuture<Boolean> result = new CompletableFuture<Boolean>();
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.addConfiguration(conf);
        clientConf.setEnsemblePlacementPolicy(LocalBookieEnsemblePlacementPolicy.class);
        clientConf.setAddEntryTimeout(cmdFlags.timeout);
        clientConf.setReadEntryTimeout(cmdFlags.timeout);

        BookKeeper bk;
        try {
            bk = new BookKeeper(clientConf);
        } catch (BKException | IOException | InterruptedException e) {
            LOG.warn("Failed to initialize bookkeeper client", e);
            result.completeExceptionally(e);
            return result;
        }

        bk.asyncCreateLedger(1, 1, BookKeeper.DigestType.MAC, new byte[0], (rc, lh, ctx) -> {
            if (rc != BKException.Code.OK) {
                LOG.warn("ledger creation failed for sanity command {}", rc);
                result.completeExceptionally(BKException.create(rc));
                return;
            }
            List<CompletableFuture<Void>> entriesFutures = new ArrayList<>();
            for (int i = 0; i < cmdFlags.entries; i++) {
                String content = "entry-" + i;
                CompletableFuture<Void> entryFuture = new CompletableFuture<>();
                entriesFutures.add(entryFuture);
                lh.asyncAddEntry(content.getBytes(UTF_8), (arc, alh, entryId, actx) -> {
                    if (arc != BKException.Code.OK) {
                        LOG.warn("ledger add entry failed for {}-{}", alh.getId(), arc);
                        entryFuture.completeExceptionally(BKException.create(arc));
                        return;
                    }
                    entryFuture.complete(null);
                }, null);
            }
            CompletableFuture<LedgerHandle> lhFuture = new CompletableFuture<>();
            CompletableFuture<Void> readEntryFuture = new CompletableFuture<>();
            FutureUtils.collect(entriesFutures).thenCompose(_r -> lh.closeAsync()).thenCompose(_r -> {
                bk.asyncOpenLedger(lh.getId(), BookKeeper.DigestType.MAC, new byte[0], (orc, olh, octx) -> {
                    if (orc != BKException.Code.OK) {
                        LOG.warn("open sanity ledger failed for {}-{}", lh.getId(), orc);
                        lhFuture.completeExceptionally(BKException.create(orc));
                        return;
                    }
                    long lac = olh.getLastAddConfirmed();
                    if (lac != (cmdFlags.entries - 1)) {
                        lhFuture.completeExceptionally(new Exception("Invalid last entry found on ledger. expecting: "
                                + (cmdFlags.entries - 1) + " -- found: " + lac));
                        return;
                    }
                    lhFuture.complete(lh);
                }, null);
                return lhFuture;
            }).thenCompose(rlh -> {
                rlh.asyncReadEntries(0, cmdFlags.entries - 1, (rrc, rlh2, entries, rctx) -> {
                    if (rrc != BKException.Code.OK) {
                        LOG.warn("reading sanity ledger failed for {}-{}", lh.getId(), rrc);
                        readEntryFuture.completeExceptionally(BKException.create(rrc));
                        return;
                    }
                    int i = 0;
                    while (entries.hasMoreElements()) {
                        LedgerEntry entry = entries.nextElement();
                        String actualMsg = new String(entry.getEntry(), UTF_8);
                        String expectedMsg = "entry-" + (i++);
                        if (!expectedMsg.equals(actualMsg)) {
                            readEntryFuture.completeExceptionally(
                                    new Exception("Failed validation of received message - Expected: " + expectedMsg
                                            + ", Actual: " + actualMsg));
                            return;
                        }
                    }
                    LOG.info("Read {} entries from ledger {}", i, lh.getId());
                    LOG.info("Bookie sanity test succeeded");
                    readEntryFuture.complete(null);
                }, null);
                return readEntryFuture;
            }).thenAccept(_r -> {
                close(bk, lh);
                result.complete(true);
            }).exceptionally(ex -> {
                close(bk, lh);
                result.completeExceptionally(ex.getCause());
                return null;
            });
        }, null);
        return result;
    }

    public static void close(BookKeeper bk, LedgerHandle lh) {
        if (lh != null) {
            bk.asyncDeleteLedger(lh.getId(), (rc, ctx) -> {
                if (rc != BKException.Code.OK) {
                    LOG.info("Failed to delete ledger id {}", lh.getId());
                }
                close(bk);
            }, null);
        } else {
            close(bk);
        }
    }

    private static void close(BookKeeper bk) {
        try {
            bk.close();
        } catch (Exception e) {
            LOG.info("Failed to close bookkeeper client {}", e.getMessage(), e);
        }
    }

}
