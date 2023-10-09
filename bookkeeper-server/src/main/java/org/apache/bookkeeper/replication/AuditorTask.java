/**
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
package org.apache.bookkeeper.replication;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AuditorTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AuditorTask.class);

    protected final ServerConfiguration conf;
    protected AuditorStats auditorStats;
    protected BookKeeperAdmin admin;
    protected LedgerManager ledgerManager;
    protected LedgerUnderreplicationManager ledgerUnderreplicationManager;
    private final ShutdownTaskHandler shutdownTaskHandler;
    private final BiConsumer<AtomicBoolean, Throwable> hasAuditCheckTask;
    private final AtomicBoolean hasTask = new AtomicBoolean(false);

    AuditorTask(ServerConfiguration conf,
                AuditorStats auditorStats,
                BookKeeperAdmin admin,
                LedgerManager ledgerManager,
                LedgerUnderreplicationManager ledgerUnderreplicationManager,
                ShutdownTaskHandler shutdownTaskHandler,
                BiConsumer<AtomicBoolean, Throwable> hasAuditCheckTask) {
        this.conf = conf;
        this.auditorStats = auditorStats;
        this.admin = admin;
        this.ledgerManager = ledgerManager;
        this.ledgerUnderreplicationManager = ledgerUnderreplicationManager;
        this.shutdownTaskHandler = shutdownTaskHandler;
        this.hasAuditCheckTask = hasAuditCheckTask;
    }

    @Override
    public void run() {
        runTask();
    }

    protected abstract void runTask();

    protected boolean isLedgerReplicationEnabled() throws ReplicationException.UnavailableException {
        return ledgerUnderreplicationManager.isLedgerReplicationEnabled();
    }

    protected CompletableFuture<?> publishSuspectedLedgersAsync(Collection<String> missingBookies, Set<Long> ledgers) {
        if (null == ledgers || ledgers.size() == 0) {
            // there is no ledgers available for this bookie and just
            // ignoring the bookie failures
            LOG.info("There is no ledgers for the failed bookie: {}", missingBookies);
            return FutureUtils.Void();
        }
        LOG.info("Following ledgers: {} of bookie: {} are identified as underreplicated", ledgers, missingBookies);
        auditorStats.getNumUnderReplicatedLedger().registerSuccessfulValue(ledgers.size());
        LongAdder underReplicatedSize = new LongAdder();
        FutureUtils.processList(
                Lists.newArrayList(ledgers),
                ledgerId ->
                        ledgerManager.readLedgerMetadata(ledgerId).whenComplete((metadata, exception) -> {
                            if (exception == null) {
                                underReplicatedSize.add(metadata.getValue().getLength());
                            }
                        }), null).whenComplete((res, e) -> {
            auditorStats.getUnderReplicatedLedgerTotalSize().registerSuccessfulValue(underReplicatedSize.longValue());
        });

        return FutureUtils.processList(
                Lists.newArrayList(ledgers),
                ledgerId -> ledgerUnderreplicationManager.markLedgerUnderreplicatedAsync(ledgerId, missingBookies),
                null
        );
    }

    protected List<String> getAvailableBookies() throws BKException {
        // Get the available bookies
        Collection<BookieId> availableBkAddresses = admin.getAvailableBookies();
        Collection<BookieId> readOnlyBkAddresses = admin.getReadOnlyBookies();
        availableBkAddresses.addAll(readOnlyBkAddresses);

        List<String> availableBookies = new ArrayList<String>();
        for (BookieId addr : availableBkAddresses) {
            availableBookies.add(addr.toString());
        }
        return availableBookies;
    }

    /**
     * Get BookKeeper client according to configuration.
     * @param conf
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    BookKeeper getBookKeeper(ServerConfiguration conf) throws IOException, InterruptedException {
        return Auditor.createBookKeeperClient(conf);
    }

    /**
     * Get BookKeeper admin according to bookKeeper client.
     * @param bookKeeper
     * @return
     */
    BookKeeperAdmin getBookKeeperAdmin(final BookKeeper bookKeeper) {
        return new BookKeeperAdmin(bookKeeper, auditorStats.getStatsLogger(), new ClientConfiguration(conf));
    }

    protected void submitShutdownTask() {
        if (shutdownTaskHandler != null) {
            shutdownTaskHandler.submitShutdownTask();
        }
    }

    public abstract void shutdown();

    protected boolean hasBookieCheckTask() {
        hasTask.set(false);
        hasAuditCheckTask.accept(hasTask, null);
        return hasTask.get();
    }

    /**
     * ShutdownTaskHandler used to shutdown auditor executor.
     */
    interface ShutdownTaskHandler {
        void submitShutdownTask();
    }

}
