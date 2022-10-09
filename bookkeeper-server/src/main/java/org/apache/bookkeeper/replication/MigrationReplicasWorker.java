/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.replication;

import static org.apache.bookkeeper.replication.ReplicationStats.NUM_ENTRIES_UNABLE_TO_READ_FOR_MIGRATION;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.bookie.BookieThread;
import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.MigrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Migrate replica data to other bookie nodes.
 */
public class MigrationReplicasWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MigrationReplicasWorker.class);
    private final BookKeeperAdmin admin;
    private final BiConsumer<Long, Long> onReadEntryFailureCallback;
    private final String lock = "locked";
    private final LedgerChecker ledgerChecker;
    private final Thread workerThread;
    private final long backoffMs = 100;
    private volatile boolean workerRunning = false;
    private final String seperator = ",";
    private final String advertisedAddress;
    private final MigrationManager migrationManager;

    @StatsDoc(
            name = NUM_ENTRIES_UNABLE_TO_READ_FOR_MIGRATION,
            help = "the number of entries MigrationReplicasWorker unable to read"
    )
    private final Counter numEntriesUnableToReadForMigration;

    public MigrationReplicasWorker(final ServerConfiguration conf, BookKeeper bkc, StatsLogger statsLogger)
            throws ReplicationException.CompatibilityException, ReplicationException.UnavailableException,
            InterruptedException {
        this.migrationManager = bkc.getLedgerManagerFactory().newMigrationManagerManager();
        this.advertisedAddress = conf.getAdvertisedAddress();
        this.ledgerChecker = new LedgerChecker(bkc);
        this.admin = new BookKeeperAdmin(bkc, statsLogger, new ClientConfiguration(conf));
        this.numEntriesUnableToReadForMigration = statsLogger
                .getCounter(NUM_ENTRIES_UNABLE_TO_READ_FOR_MIGRATION);
        this.onReadEntryFailureCallback = (ledgerid, entryid) -> {
            numEntriesUnableToReadForMigration.inc();
        };
        this.workerThread = new BookieThread(this, "MigrationReplicasWorker");
    }

    static class Replicas {
        long ledgerId;
        Set<BookieId> bookieIds;

        public Replicas(long ledgerId, Set<BookieId> bookieIds) {
            this.ledgerId = ledgerId;
            this.bookieIds = bookieIds;
        }

        @Override
        public String toString() {
            return String.format("<ledgerId=%s,bookies=<%s>", ledgerId, bookieIds);
        }
    }

    private Optional<Replicas> getReplicasToMigrate(List<String> ledgerIds) throws InterruptedException, KeeperException {
        Iterator<String> iterator = ledgerIds.iterator();
        while (iterator.hasNext()) {
            try {
                long ledgerId = Long.parseLong(iterator.next());
                if (!migrationManager.exists(ledgerId)) {
                    // The ledger migration is completed
                    iterator.remove();
                    continue;
                }
                String bookieIdStr = migrationManager.
                        getOwnerBookiesMigrationReplicas(ledgerId);
                migrationManager.lockMigrationReplicas(ledgerId, advertisedAddress);
                String[] migrationBookieIds = bookieIdStr.split(seperator);
                Set<BookieId> bookieIds = new HashSet<>();
                for (int i = 0; i < migrationBookieIds.length; i++) {
                    bookieIds.add(BookieId.parse(migrationBookieIds[i]));
                }

                return Optional.of(new Replicas(ledgerId, bookieIds));
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have locked the ledger
            } catch (UnsupportedEncodingException e) {
                LOG.error("The encoding is not supported!", e);
            }
        }
        return Optional.empty();
    }

    private Set<LedgerFragment> getFragmentsOnMigrationBookies(LedgerHandle lh, Set<BookieId> migrationBookieIds) {
        return ledgerChecker.getFragmentsOnMigratedBookies(lh, migrationBookieIds);
    }

    private static void waitBackOffTime(long backoffMs) {
        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Start the migration replicas worker.
     */
    public void start() {
        this.workerThread.start();
    }

    @Override
    public void run() {
        workerRunning = true;
        List<String> toMigrateLedgerIds = Collections.emptyList();
        while (workerRunning) {
            // 1. build migrating ledgers
            try {
                if (toMigrateLedgerIds.isEmpty()) {
                    toMigrateLedgerIds = migrationManager.listLedgersOfMigrationReplicas();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (Throwable e) {
                LOG.error("list migrating ledgers failed!", e);
            }

            // 2. Get a replica ledger to replicate or wait back off
            Optional<Replicas> replicasOp = Optional.empty();
            try {
                replicasOp = getReplicasToMigrate(toMigrateLedgerIds);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (Throwable e) {
                LOG.error("Get migrating replicas failed!", e);
            }
            if (replicasOp.isEmpty()) {
                waitBackOffTime(backoffMs);
                continue;
            }

            // 3. Get the fragment containing the migration bookie
            Replicas replicas = replicasOp.get();
            LOG.info(String.format("Start migrate replicas(%s)!", replicas));
            boolean released = false;
            try (LedgerHandle lh = admin.openLedgerNoRecovery(replicas.ledgerId)) {
                Set<LedgerFragment> fragments = getFragmentsOnMigrationBookies(lh, replicas.bookieIds);
                if (fragments.size() < 1) {
                    //3.The replication has been completed, delete the ledgerId directory and release lock
                    migrationManager.releaseLock(replicas.ledgerId);
                    migrationManager.deleteMigrationLedgerPath(replicas.ledgerId);
                    released = true;
                    LOG.info(String.format("Finish ledgerId %s migration!", replicas.ledgerId));
                }

                // 4. Start replicate
                for (LedgerFragment ledgerFragment : fragments) {
                    admin.replicateLedgerFragment(lh, ledgerFragment, onReadEntryFailureCallback);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (Throwable e) {
                LOG.error(String.format("LedgerId %s migrate failed!", replicas.ledgerId), e);
            } finally {
                // 5. release lock
                if (!released) {
                    migrationManager.releaseLock(replicas.ledgerId);
                }
            }
        }
        LOG.info("MigrationReplicasWorker exited loop!");
    }

    public void shutdown() {
        LOG.info("Shutting down migration replicas worker");
        synchronized (this) {
            if (!workerRunning) {
                return;
            }
            workerRunning = false;
        }
        migrationManager.close();
        LOG.info("Shutdown finished!");
    }
}
