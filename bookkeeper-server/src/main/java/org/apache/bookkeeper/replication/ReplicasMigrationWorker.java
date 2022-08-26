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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.bookie.BookieThread;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerChecker;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Migrate replica data to other bookie nodes.
 */
public class ReplicasMigrationWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicasMigrationWorker.class);
    private final BookKeeperAdmin admin;
    private final BiConsumer<Long, Long> onReadEntryFailureCallback;
    private final MetadataClientDriver metadataClientDriver;
    private final String rootPath;
    private final String replicasMigrationPath;
    private final String lock = "locked";
    private final LedgerChecker ledgerChecker;
    private final Thread workerThread;
    private final long backoffMs = 100;
    private volatile boolean workerRunning = false;
    private final String seperator = ",";
    private final String advertisedAddress;

    @StatsDoc(
            name = NUM_ENTRIES_UNABLE_TO_READ_FOR_MIGRATION,
            help = "the number of entries ReplicasMigrationWorker unable to read"
    )
    private final Counter numEntriesUnableToReadForMigration;

    public ReplicasMigrationWorker(final ServerConfiguration conf, BookKeeper bkc, StatsLogger statsLogger) {
        this.advertisedAddress = conf.getAdvertisedAddress();
        this.rootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf);
        this.replicasMigrationPath = getReplicasMigrationPath(rootPath);
        this.ledgerChecker = new LedgerChecker(bkc);
        this.admin = new BookKeeperAdmin(bkc, statsLogger, new ClientConfiguration(conf));
        this.metadataClientDriver = bkc.getMetadataClientDriver();
        this.numEntriesUnableToReadForMigration = statsLogger
                .getCounter(NUM_ENTRIES_UNABLE_TO_READ_FOR_MIGRATION);
        this.onReadEntryFailureCallback = (ledgerid, entryid) -> {
            numEntriesUnableToReadForMigration.inc();
        };
        this.workerThread = new BookieThread(this, "ReplicasMigrationWorker");
    }

    public static String getReplicasMigrationPath(String rootPath) {
        return String.format("%s/%s", rootPath, BookKeeperConstants.MIGRATION_REPLICAS);
    }

    public static String getLockForMigrationReplicasPath(String replicasMigrationPath, long ledgerId, String lock) {
        return String.format("%s/%s/%s", replicasMigrationPath, ledgerId, lock);
    }

    public static String getLedgerForMigrationReplicasPath(String replicasMigrationPath, long ledgerId) {
        return String.format("%s/%s", replicasMigrationPath, ledgerId);
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

    private Replicas getReplicasToMigrate(List<String> ledgerIds) throws InterruptedException, KeeperException {
        Iterator<String> iterator = ledgerIds.iterator();
        while (iterator.hasNext()) {
            try {
                String ledgerId = iterator.next();
                String ledgerForMigrationReplicasPath = getLedgerForMigrationReplicasPath(replicasMigrationPath,
                        Long.parseLong(ledgerId));
                if (!metadataClientDriver.exists(ledgerForMigrationReplicasPath)) {
                    // The ledger migration is completed
                    iterator.remove();
                    continue;
                }
                String bookieIdStr = metadataClientDriver.
                        getOwnerBookiesMigrationReplicas(ledgerForMigrationReplicasPath);

                String lockForMigrationReplicasPath = getLockForMigrationReplicasPath(replicasMigrationPath,
                        Long.parseLong(ledgerId), lock);
                metadataClientDriver.lockMigrationReplicas(lockForMigrationReplicasPath, advertisedAddress);
                String[] migrationBookieIds = bookieIdStr.split(seperator);
                Set<BookieId> bookieIds = new HashSet<>();
                for (int i = 0; i < migrationBookieIds.length; i++) {
                    bookieIds.add(BookieId.parse(migrationBookieIds[i]));
                }
                return new Replicas(Long.parseLong(ledgerId), bookieIds);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have locked the ledger
            } catch (UnsupportedEncodingException e) {
                LOG.error("The encoding is not supported!", e);
            }
        }
        return null;
    }

    private Set<LedgerFragment> getFragmentsOnMigrationBookies(LedgerHandle lh, Set<BookieId> migrationBookieIds) {
        return ledgerChecker.getFragmentsOnMigrationBookies(lh, migrationBookieIds);
    }

    private static void waitBackOffTime(long backoffMs) {
        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void releaseLock(Replicas replicas) {
        try {
            metadataClientDriver.deleteZkPath(getLockForMigrationReplicasPath(replicasMigrationPath,
                    replicas.ledgerId, lock));
            LOG.info(String.format("Release lock for ledgerId %s success!", replicas.ledgerId));
        } catch (KeeperException.NoNodeException e) {
            // do nothing,already release lock
        } catch (Exception e) {
            LOG.error(String.format("Release lock for ledgerId %s failed!", replicas.ledgerId));
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
                    toMigrateLedgerIds = metadataClientDriver.listLedgersOfMigrationReplicas(replicasMigrationPath);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (Throwable e) {
                LOG.error("list migrating ledgers failed!", e);
            }

            // 2. Get a replica ledger to replicate or wait back off
            Replicas replicas = null;
            try {
                replicas = getReplicasToMigrate(toMigrateLedgerIds);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (Throwable e) {
                LOG.error("Get migrating replicas failed!", e);
            }
            if (null == replicas) {
                waitBackOffTime(backoffMs);
                continue;
            }

            // 3. Get the fragment containing the migration bookie
            LOG.info(String.format("Start migrate replicas(%s)!", replicas));
            try (LedgerHandle lh = admin.openLedgerNoRecovery(replicas.ledgerId)) {
                Set<LedgerFragment> fragments = getFragmentsOnMigrationBookies(lh, replicas.bookieIds);
                if (fragments.size() < 1) {
                    //3.The replication has been completed, delete the ledgerId directory and release lock
                    releaseLock(replicas);
                    metadataClientDriver.deleteZkPath(getLedgerForMigrationReplicasPath(replicasMigrationPath,
                            replicas.ledgerId));
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
                releaseLock(replicas);
            }
        }
        LOG.info("ReplicasMigrationWorker exited loop!");
    }
}
