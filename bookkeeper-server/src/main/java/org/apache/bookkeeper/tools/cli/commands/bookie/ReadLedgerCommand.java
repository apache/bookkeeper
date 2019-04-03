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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.LongStream;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to read ledger entries.
 */
public class ReadLedgerCommand extends BookieCommand<ReadLedgerCommand.ReadLedgerFlags> {

    static final Logger LOG = LoggerFactory.getLogger(ReadLedgerCommand.class);

    private static final String NAME = "readledger";
    private static final String DESC = "Read a range of entries from a ledger.";

    EntryFormatter entryFormatter;
    LedgerIdFormatter ledgerIdFormatter;

    public ReadLedgerCommand() {
        this(new ReadLedgerFlags());
    }

    public ReadLedgerCommand(EntryFormatter entryFormatter, LedgerIdFormatter ledgerIdFormatter) {
        this(new ReadLedgerFlags());
        this.ledgerIdFormatter = ledgerIdFormatter;
        this.entryFormatter = entryFormatter;
    }

    private ReadLedgerCommand(ReadLedgerFlags flags) {
        super(CliSpec.<ReadLedgerFlags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    /**
     * Flags for read ledger command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class ReadLedgerFlags extends CliFlags {

        @Parameter(names = { "-m", "--msg" }, description = "Print message body")
        private boolean msg;

        @Parameter(names = { "-l", "--ledgerid" }, description = "Ledger ID")
        private long ledgerId = -1;

        @Parameter(names = { "-fe", "--firstentryid" }, description = "First Entry ID")
        private long firstEntryId = -1;

        @Parameter(names = { "-le", "--lastentryid" }, description = "Last Entry ID")
        private long lastEntryId = -1;

        @Parameter(names = { "-r", "--force-recovery" },
            description = "Ensure the ledger is properly closed before reading")
        private boolean forceRecovery;

        @Parameter(names = { "-b", "--bookie" }, description = "Only read from a specific bookie")
        private String bookieAddresss;

        @Parameter(names = { "-lf", "--ledgeridformatter" }, description = "Set ledger id formatter")
        private String ledgerIdFormatter;

        @Parameter(names = { "-ef", "--entryformatter" }, description = "Set entry formatter")
        private String entryFormatter;
    }

    @Override
    public boolean apply(ServerConfiguration conf, ReadLedgerFlags cmdFlags) {
        if (cmdFlags.ledgerIdFormatter != null && ledgerIdFormatter == null) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(cmdFlags.ledgerIdFormatter, conf);
        } else if (ledgerIdFormatter == null) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(conf);
        }

        if (cmdFlags.entryFormatter != null && entryFormatter == null) {
            this.entryFormatter = EntryFormatter.newEntryFormatter(cmdFlags.entryFormatter, conf);
        } else if (entryFormatter == null) {
            this.entryFormatter = EntryFormatter.newEntryFormatter(conf);
        }

        try {
            return readledger(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean readledger(ServerConfiguration serverConf, ReadLedgerFlags flags)
        throws InterruptedException, BKException, IOException {

        long lastEntry = flags.lastEntryId;

        final BookieSocketAddress bookie;
        if (flags.bookieAddresss != null) {
            // A particular bookie was specified
            bookie = new BookieSocketAddress(flags.bookieAddresss);
        } else {
            bookie = null;
        }

        ClientConfiguration conf = new ClientConfiguration();
        conf.addConfiguration(serverConf);

        try (BookKeeperAdmin bk = new BookKeeperAdmin(conf)) {
            if (flags.forceRecovery) {
                // Force the opening of the ledger to trigger recovery
                try (LedgerHandle lh = bk.openLedger(flags.ledgerId)) {
                    if (lastEntry == -1 || lastEntry > lh.getLastAddConfirmed()) {
                        lastEntry = lh.getLastAddConfirmed();
                    }
                }
            }

            if (bookie == null) {
                // No bookie was specified, use normal bk client
                Iterator<LedgerEntry> entries = bk.readEntries(flags.ledgerId, flags.firstEntryId, lastEntry)
                                                  .iterator();
                while (entries.hasNext()) {
                    LedgerEntry entry = entries.next();
                    formatEntry(entry, flags.msg);
                }
            } else {
                // Use BookieClient to target a specific bookie
                EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
                OrderedExecutor executor = OrderedExecutor.newBuilder()
                                                          .numThreads(1)
                                                          .name("BookieClientScheduler")
                                                          .build();

                ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("BookKeeperClientSchedulerPool"));

                BookieClient bookieClient = new BookieClientImpl(conf, eventLoopGroup, UnpooledByteBufAllocator.DEFAULT,
                                                                 executor, scheduler, NullStatsLogger.INSTANCE);

                LongStream.range(flags.firstEntryId, lastEntry).forEach(entryId -> {
                    CompletableFuture<Void> future = new CompletableFuture<>();

                    bookieClient.readEntry(bookie, flags.ledgerId, entryId,
                                           (rc, ledgerId1, entryId1, buffer, ctx) -> {
                                               if (rc != BKException.Code.OK) {
                                                   LOG.error("Failed to read entry {} -- {}", entryId1,
                                                             BKException.getMessage(rc));
                                                   future.completeExceptionally(BKException.create(rc));
                                                   return;
                                               }

                                               System.out.println(
                                                   "--------- Lid=" + ledgerIdFormatter.formatLedgerId(flags.ledgerId)
                                                   + ", Eid=" + entryId + " ---------");
                                               if (flags.msg) {
                                                   System.out.println("Data: " + ByteBufUtil.prettyHexDump(buffer));
                                               }

                                               future.complete(null);
                                           }, null, BookieProtocol.FLAG_NONE);

                    try {
                        future.get();
                    } catch (Exception e) {
                        LOG.error("Error future.get while reading entries from ledger {}", flags.ledgerId, e);
                    }
                });

                eventLoopGroup.shutdownGracefully();
                executor.shutdown();
                bookieClient.close();
            }
        }
        return true;
    }

    /**
     * Format the entry into a readable format.
     *
     * @param entry
     *          ledgerentry to print
     * @param printMsg
     *          Whether printing the message body
     */
    private void formatEntry(LedgerEntry entry, boolean printMsg) {
        long ledgerId = entry.getLedgerId();
        long entryId = entry.getEntryId();
        long entrySize = entry.getLength();
        System.out.println("--------- Lid=" + ledgerIdFormatter.formatLedgerId(ledgerId) + ", Eid=" + entryId
                           + ", EntrySize=" + entrySize + " ---------");
        if (printMsg) {
            entryFormatter.formatEntry(entry.getEntry());
        }
    }
}
