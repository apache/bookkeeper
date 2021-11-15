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
import com.google.common.base.Functions;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieId;
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
            final BookKeeperAdmin admin = new BookKeeperAdmin(clientConf);
            return relocate(conf, flags, bookKeeper, admin);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    @VisibleForTesting
    public boolean relocate(ServerConfiguration conf, CorrectEnsemblePlacementFlags flags,
                            BookKeeper bookKeeper, BookKeeperAdmin admin) throws Exception {
        final EnsemblePlacementPolicy placementPolicy = bookKeeper.getPlacementPolicy();

        @Cleanup
        final MetadataBookieDriver metadataDriver = instantiateMetadataDriver(conf);
        @Cleanup
        final LedgerManagerFactory lmf = metadataDriver.getLedgerManagerFactory();
        @Cleanup
        final LedgerUnderreplicationManager lum = lmf.newLedgerUnderreplicationManager();

        final List<Long> targetLedgers =
                flags.ledgerIds.stream().parallel().distinct().filter(ledgerId -> {
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

                    final LedgerMetadata ledgerMeta = lh.getLedgerMetadata();
                    final Map<Long, Long> ledgerFragmentsRange = new HashMap<>();
                    Long curEntryId = null;
                    for (Map.Entry<Long, ? extends List<BookieId>> entry :
                            ledgerMeta.getAllEnsembles().entrySet()) {
                        if (curEntryId != null) {
                            ledgerFragmentsRange.put(curEntryId, entry.getKey() - 1);
                        }
                        curEntryId = entry.getKey();
                    }
                    if (curEntryId != null) {
                        ledgerFragmentsRange.put(curEntryId, lh.getLastAddConfirmed());
                    }

                    for (Map.Entry<Long, ? extends List<BookieId>> entry : ledgerMeta.getAllEnsembles().entrySet()) {
                        if (placementPolicy.isEnsembleAdheringToPlacementPolicy(entry.getValue(),
                                ledgerMeta.getWriteQuorumSize(), ledgerMeta.getAckQuorumSize())
                                == EnsemblePlacementPolicy.PlacementPolicyAdherence.FAIL) {
                            try {
                                final List<BookieId> currentEnsemble =  entry.getValue();
                                // Currently, don't consider quarantinedBookies
                                final EnsemblePlacementPolicy.PlacementResult<List<BookieId>> placementResult =
                                        placementPolicy.replaceToAdherePlacementPolicy(
                                                ledgerMeta.getEnsembleSize(),
                                                ledgerMeta.getWriteQuorumSize(),
                                                ledgerMeta.getAckQuorumSize(),
                                                Collections.emptySet(),
                                                currentEnsemble);

                                if (placementResult.isAdheringToPolicy()
                                        == EnsemblePlacementPolicy.PlacementPolicyAdherence.FAIL) {
                                    LOG.warn("Failed to relocate the ensemble. So, skip the operation."
                                                    + " ledgerId: {}, fragmentIndex: {}",
                                            ledgerId, entry.getKey());
                                    failedTargets.add(Pair.of(ledgerId, entry.getKey()));
                                } else {
                                    final List<BookieId> newEnsemble = placementResult.getResult();
                                    final Map<Integer, BookieId> replaceBookiesMap = IntStream
                                            .range(0, ledgerMeta.getEnsembleSize()).boxed()
                                            .filter(i -> !newEnsemble.get(i).equals(currentEnsemble.get(i)))
                                            .collect(Collectors.toMap(Functions.identity(), newEnsemble::get));
                                    if (replaceBookiesMap.isEmpty()) {
                                        LOG.warn("Failed to get bookies to replace. So, skip the operation."
                                                        + " ledgerId: {}, fragmentIndex: {}",
                                                ledgerId, entry.getKey());
                                        failedTargets.add(Pair.of(ledgerId, entry.getKey()));
                                    } else if (flags.dryRun) {
                                        LOG.info("Would replace the ensemble. ledgerId: {}, fragmentIndex: {},"
                                                        + " currentEnsemble: {} replaceBookiesMap {}",
                                                ledgerId, entry.getKey(),
                                                currentEnsemble, replaceBookiesMap);
                                    } else {
                                        if (LOG.isDebugEnabled()) {
                                            LOG.debug("Try to replace the ensemble. ledgerId: {}, fragmentIndex: {},"
                                                            + " replaceBookiesMap {}",
                                                    ledgerId, entry.getKey(), replaceBookiesMap);
                                        }
                                        final LedgerFragment fragment = new LedgerFragment(lh, entry.getKey(),
                                                ledgerFragmentsRange.get(entry.getKey()), replaceBookiesMap.keySet());

                                        try {
                                            admin.replicateLedgerFragment(lh, fragment, replaceBookiesMap,
                                                    (lId, eId) -> {
                                                        // This consumer is already accepted before the method returns
                                                        // void. Therefore, use failedTargets in this consumer.
                                                        LOG.warn("Failed to read entry {}:{}", lId, eId);
                                                        failedTargets.add(Pair.of(ledgerId, entry.getKey()));
                                                    });
                                            LOG.info("Operation finished in the ensemble. ledgerId: {},"
                                                            + " fragmentIndex: {}, replaceBookiesMap {}",
                                                    ledgerId, entry.getKey(), replaceBookiesMap);
                                        } catch (BKException | InterruptedException e) {
                                            LOG.warn("Failed to replicate ledger fragment.", e);
                                            failedTargets.add(Pair.of(ledgerId, entry.getKey()));
                                        }
                                    }
                                }
                            } catch (UnsupportedOperationException e) {
                                LOG.warn("UnsupportedOperationException caught. The placement policy might not support"
                                        + " replaceToAdherePlacementPolicy method.", e);
                                failedTargets.add(Pair.of(ledgerId, entry.getKey()));
                            }
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("The fragment is adhering to placement policy. So, skip the operation."
                                        + " ledgerId: {}, fragmentIndex: {}", ledgerId, entry.getKey());
                            }
                        }
                    }
                } finally {
                    try {
                        if (!flags.dryRun) {
                            lum.releaseUnderreplicatedLedger(ledgerId);
                        }
                    } catch (ReplicationException e) {
                        LOG.warn("Failed to release under replicated ledger {}.", ledgerId, e);
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
