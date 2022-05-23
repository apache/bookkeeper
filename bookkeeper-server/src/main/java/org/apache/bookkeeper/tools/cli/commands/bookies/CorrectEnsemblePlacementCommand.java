/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.tools.cli.commands.bookies;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.CommaParameterSplitter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to relocate ledgers to adhere ensemble placement policy.
 */
public class CorrectEnsemblePlacementCommand extends
        BookieCommand<CorrectEnsemblePlacementCommand.CorrectEnsemblePlacementFlags> {
    private static final Logger LOG = LoggerFactory.getLogger(CorrectEnsemblePlacementCommand.class);

    private static final String NAME = "correct-ensemble-placement";
    private static final String DESC = "Relocate ledgers to adhere ensemble placement policy.";

    public CorrectEnsemblePlacementCommand() {
        this(new CorrectEnsemblePlacementFlags());
    }

    private CorrectEnsemblePlacementCommand(CorrectEnsemblePlacementFlags flags) {
        super(CliSpec.<CorrectEnsemblePlacementFlags>newBuilder()
                .withName(NAME)
                .withDescription(DESC)
                .withFlags(flags)
                .build());
    }

    /**
     * Flags for correct-ensemble-placement command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class CorrectEnsemblePlacementFlags extends CliFlags {
        @Parameter(names = { "-l", "--ledgerids" },
                description = "Target ledger IDs to relocate. Multiple can be specified, comma separated.",
                splitter = CommaParameterSplitter.class, required = true)
        private List<Long> ledgerIds;

        @Parameter(names = { "-dr", "--dryrun" },
                description = "Printing the relocation plan w/o doing actual relocation")
        private boolean dryRun;

        @Parameter(names = { "-f", "--force" }, description = "Force relocation without confirmation")
        private boolean force;

        @Parameter(names = {"-sk", "--skipOpenLedgers"}, description = "Skip relocating open ledgers")
        private boolean skipOpenLedgers;
    }

    @Override
    public boolean apply(ServerConfiguration conf, CorrectEnsemblePlacementFlags flags) {
        try {
            if (flags.dryRun) {
                LOG.info("The dry-run output could change every time you run"
                        + " since the selection of bookies replaced includes some randomness.");
            }
            if (!flags.skipOpenLedgers) {
                LOG.warn("Try to relocate also open ledgers. It is not recommended.");
            }
            try {
                if (!flags.force) {
                    final boolean confirm =
                            IOUtils.confirmPrompt("Are you sure to relocate target ledgers?");
                    if (!confirm) {
                        LOG.error("Relocation is aborted.");
                        return false;
                    }
                }
            } catch (IOException e) {
                LOG.error("Error during relocation", e);
                return false;
            }

            final ClientConfiguration clientConf = new ClientConfiguration(conf);
            final BookKeeper bookKeeper = new BookKeeper(clientConf);
            final BookKeeperAdmin admin = new BookKeeperAdmin(bookKeeper);
            return relocate(conf, flags, bookKeeper, admin);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    @VisibleForTesting
    public boolean relocate(ServerConfiguration conf, CorrectEnsemblePlacementFlags flags,
                            BookKeeper bookKeeper, BookKeeperAdmin admin) throws Exception {
        @Cleanup
        final MetadataBookieDriver metadataDriver = instantiateMetadataDriver(conf);
        @Cleanup
        final LedgerManagerFactory lmf = metadataDriver.getLedgerManagerFactory();
        @Cleanup
        final LedgerUnderreplicationManager lum = lmf.newLedgerUnderreplicationManager();

        final List<Long> targetLedgers =
                flags.ledgerIds.stream().distinct().filter(ledgerId -> {
                    try {
                        return (!flags.skipOpenLedgers || bookKeeper.isClosed(ledgerId))
                                && !lum.isLedgerBeingReplicated(ledgerId);
                    } catch (BKException | InterruptedException | ReplicationException e) {
                        LOG.warn("Failed to add the ledger {} to target.", ledgerId, e);
                        return false;
                    }
                }).collect(Collectors.toList());

        if (targetLedgers.isEmpty()) {
            LOG.info("None of ledgers are relocated.");
            return true;
        }

        final NavigableSet<Long> unAcquirableLedgers = new TreeSet<>();
        final NavigableSet<Pair<Long, Long>> failedTargets = new ConcurrentSkipListSet<>();
        final CountDownLatch latch = new CountDownLatch(targetLedgers.size());
        for (long ledgerId : targetLedgers) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Start relocation of the ledger {}.", ledgerId);
            }
            if (!flags.dryRun) {
                try {
                    lum.acquireUnderreplicatedLedger(ledgerId);
                } catch (ReplicationException e) {
                    LOG.warn("Failed to acquire ledger to under replicated {}.", ledgerId);
                    unAcquirableLedgers.add(ledgerId);
                    latch.countDown();
                    continue;
                }
            }
            admin.asyncOpenLedger(ledgerId, (rc, lh, ctx) -> {
                try {
                    if (rc != BKException.Code.OK) {
                        LOG.warn("Failed to open ledger {}", ledgerId);
                        return;
                    }
                    admin.relocateLedgerToAdherePlacementPolicy(lh, flags.dryRun)
                            .forEach(e -> failedTargets.add(Pair.of(ledgerId, e)));
                } catch (UnsupportedOperationException e) {
                    LOG.warn("UnsupportedOperationException caught. The placement policy might not support"
                            + " replaceToAdherePlacementPolicy method.", e);
                } finally {
                    try {
                        if (!flags.dryRun) {
                            lum.releaseUnderreplicatedLedger(ledgerId);
                        }
                    } catch (ReplicationException e) {
                        LOG.error("Failed to release under replicated ledger {}.", ledgerId, e);
                    } finally {
                        ((CountDownLatch) ctx).countDown();
                    }
                }
            }, latch);
        }

        // Currently, don't add timeout
        latch.await();

        if (unAcquirableLedgers.isEmpty() && failedTargets.isEmpty()) {
            return true;
        } else {
            LOG.warn("Some ensembles couldn't be relocated to adhere placement policy."
                    + " Un-acquirable ledgers: {}"
                    + ", Failed targets [(ledgerId, fragmentIndex), ...]: {}", unAcquirableLedgers, failedTargets);
            return false;
        }
    }

    private static MetadataBookieDriver instantiateMetadataDriver(ServerConfiguration conf)
            throws BookieException {
        try {
            final String metadataServiceUriStr = conf.getMetadataServiceUri();
            final MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(URI.create(metadataServiceUriStr));
            driver.initialize(conf, NullStatsLogger.INSTANCE);
            return driver;
        } catch (MetadataException me) {
            throw new BookieException.MetadataStoreException("Failed to initialize metadata bookie driver", me);
        } catch (ConfigurationException e) {
            throw new BookieException.BookieIllegalOpException(e);
        }
    }
}
