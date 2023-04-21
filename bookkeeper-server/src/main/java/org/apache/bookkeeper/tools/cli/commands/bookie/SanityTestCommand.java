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
import java.util.Enumeration;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.LocalBookieEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
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

    private boolean handle(ServerConfiguration conf, SanityFlags cmdFlags) throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.addConfiguration(conf);
        clientConf.setEnsemblePlacementPolicy(LocalBookieEnsemblePlacementPolicy.class);
        clientConf.setAddEntryTimeout(cmdFlags.timeout);
        clientConf.setReadEntryTimeout(cmdFlags.timeout);

        BookKeeper bk = new BookKeeper(clientConf);
        LedgerHandle lh = null;
        try {
            lh = bk.createLedger(1, 1, BookKeeper.DigestType.MAC, new byte[0]);
            LOG.info("Create ledger {}", lh.getId());

            for (int i = 0; i < cmdFlags.entries; i++) {
                String content = "entry-" + i;
                lh.addEntry(content.getBytes(UTF_8));
            }

            LOG.info("Written {} entries in ledger {}", cmdFlags.entries, lh.getId());

            // Reopen the ledger and read entries
            lh = bk.openLedger(lh.getId(), BookKeeper.DigestType.MAC, new byte[0]);
            if (lh.getLastAddConfirmed() != (cmdFlags.entries - 1)) {
                throw new Exception("Invalid last entry found on ledger. expecting: " + (cmdFlags.entries - 1)
                                        + " -- found: " + lh.getLastAddConfirmed());
            }

            Enumeration<LedgerEntry> entries = lh.readEntries(0, cmdFlags.entries - 1);
            int i = 0;
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                String actualMsg = new String(entry.getEntry(), UTF_8);
                String expectedMsg = "entry-" + (i++);
                if (!expectedMsg.equals(actualMsg)) {
                    throw new Exception("Failed validation of received message - Expected: " + expectedMsg
                                            + ", Actual: " + actualMsg);
                }
            }

            LOG.info("Read {} entries from ledger {}", i, lh.getId());
        } catch (Exception e) {
            LOG.warn("Error in bookie sanity test", e);
            return false;
        } finally {
            if (lh != null) {
                bk.deleteLedger(lh.getId());
                LOG.info("Deleted ledger {}", lh.getId());
            }

            bk.close();
        }

        LOG.info("Bookie sanity test succeeded");
        return true;
    }
}
