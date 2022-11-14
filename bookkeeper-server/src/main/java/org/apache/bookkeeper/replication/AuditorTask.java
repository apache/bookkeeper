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
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AuditorTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AuditorTask.class);

    protected final Auditor auditor;
    protected final ServerConfiguration conf;
    protected AuditorStats auditorStats;
    protected BookKeeperAdmin admin;
    protected LedgerManager ledgerManager;
    protected LedgerUnderreplicationManager ledgerUnderreplicationManager;
    private Auditor.ShutdownTaskHandler shutdownTaskHandler;

    AuditorTask(Auditor auditor,
                ServerConfiguration conf,
                AuditorStats auditorStats,
                BookKeeperAdmin admin,
                LedgerManager ledgerManager,
                LedgerUnderreplicationManager ledgerUnderreplicationManager,
                Auditor.ShutdownTaskHandler shutdownTaskHandler) {
        this.auditor = auditor;
        this.conf = conf;
        this.auditorStats = auditorStats;
        this.admin = admin;
        this.ledgerManager = ledgerManager;
        this.ledgerUnderreplicationManager = ledgerUnderreplicationManager;
        this.shutdownTaskHandler = shutdownTaskHandler;
    }

    @Override
    public void run() {
        runTask();
    }

    protected abstract void runTask();

    protected void waitIfLedgerReplicationDisabled() throws ReplicationException.UnavailableException,
            InterruptedException {
        ReplicationEnableCb cb = new ReplicationEnableCb();
        if (!isLedgerReplicationEnabled()) {
            LOG.info("LedgerReplication is disabled externally through Zookeeper, "
                    + "since DISABLE_NODE ZNode is created, so waiting untill it is enabled");
            ledgerUnderreplicationManager.notifyLedgerReplicationEnabled(cb);
            cb.await();
        }
    }

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

    protected void submitShutdownTask() {
        shutdownTaskHandler.submitShutdownTask();
    }

}
