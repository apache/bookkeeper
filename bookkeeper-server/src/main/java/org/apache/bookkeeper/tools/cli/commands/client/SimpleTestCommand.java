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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.tools.cli.commands.client.SimpleTestCommand.Flags;
import org.apache.bookkeeper.tools.cli.helpers.ClientCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client command that simply tests if a cluster is healthy.
 */
public class SimpleTestCommand extends ClientCommand<Flags> {

    private static final String NAME = "simpletest";
    private static final String DESC = "Simple test to create a ledger and write entries to it, then read it.";
    private static final Logger LOG = LoggerFactory.getLogger(SimpleTestCommand.class);

    /**
     * Flags for simple test command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class Flags extends CliFlags {

        @Parameter(names = { "-e", "--ensemble-size" }, description = "Ensemble size (default 3)")
        private int ensembleSize = 3;
        @Parameter(names = { "-w", "--write-quorum-size" }, description = "Write quorum size (default 2)")
        private int writeQuorumSize = 2;
        @Parameter(names = { "-a", "--ack-quorum-size" }, description = "Ack quorum size (default 2)")
        private int ackQuorumSize = 2;
        @Parameter(names = { "-n", "--num-entries" }, description = "Entries to write (default 100)")
        private int numEntries = 100;

    }
    public SimpleTestCommand() {
        this(new Flags());
    }

    public SimpleTestCommand(Flags flags) {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(flags)
            .build());
    }

    @Override
    @SuppressFBWarnings({"RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "DMI_RANDOM_USED_ONLY_ONCE"})
    protected void run(BookKeeper bk, Flags flags) throws Exception {
        byte[] data = new byte[100]; // test data
        Random random = new Random(0);
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (random.nextInt(26) + 65);
        }
        WriteHandle wh = null;
        try {
            wh = result(bk.newCreateLedgerOp()
                    .withEnsembleSize(flags.ensembleSize)
                    .withWriteQuorumSize(flags.writeQuorumSize)
                    .withAckQuorumSize(flags.ackQuorumSize)
                    .withDigestType(DigestType.CRC32C)
                    .withCustomMetadata(ImmutableMap.of("Bookie", NAME.getBytes(StandardCharsets.UTF_8)))
                    .withPassword(new byte[0])
                    .execute());
            LOG.info("Ledger ID: {}", wh.getId());
            long lastReport = System.nanoTime();
            for (int i = 0; i < flags.numEntries; i++) {
                wh.append(data);
                if (TimeUnit.SECONDS.convert(System.nanoTime() - lastReport,
                        TimeUnit.NANOSECONDS) > 1) {
                    LOG.info("{} entries written", i);
                    lastReport = System.nanoTime();
                }
            }
            LOG.info("{} entries written to ledger {}", flags.numEntries, wh.getId());

            try (ReadHandle rh = result(bk.newOpenLedgerOp().withLedgerId(wh.getId()).withDigestType(DigestType.CRC32C)
                    .withPassword(new byte[0]).execute())) {
                LedgerEntries ledgerEntries = rh.read(0, flags.numEntries);
                for (LedgerEntry ledgerEntry : ledgerEntries) {
                    if (!Arrays.equals(ledgerEntry.getEntryBytes(), data)) {
                        LOG.error("Read test failed, the reading data is not equals writing data.");
                    }
                }
            }
        } finally {
            if (wh != null) {
                wh.close();
                result(bk.newDeleteLedgerOp().withLedgerId(wh.getId()).execute());
            }
        }
    }
}
