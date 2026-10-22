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
package org.apache.bookkeeper.replication;

import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Test;

/**
 * Focused unit test for {@link AuditorReplicasCheckTask} that pins down a cross-ledger-range
 * accounting bug: the {@code numLedgersFoundHaving*} counters are reset at the top of every
 * {@code LedgerRange} iteration inside {@code replicasCheck()}, but the gauges are published once
 * after the whole run. As a result, a finding in an earlier range is wiped by a later (healthy)
 * range and never reported.
 *
 * <p>This test drives {@code replicasCheck} entirely with mocked collaborators (no cluster), so the
 * mocked futures complete inline and the scenario is fully deterministic.
 *
 * <p>Expected behaviour: ledger A (in range 1) has an entry with fewer than write-quorum replicas,
 * so {@code NUM_LEDGERS_HAVING_LESS_THAN_WQ_REPLICAS_OF_AN_ENTRY} must be 1. On the unpatched code
 * the second (healthy) range resets the counter to 0 before the gauge is read, so the assertion
 * fails with actual 0.
 */
public class AuditorReplicasCheckTaskCrossRangeTest {

    private static final long LEDGER_A = 1L; // unhealthy, lands in the first ledger range
    private static final long LEDGER_B = 2L; // healthy, lands in a later ledger range

    private static AvailabilityOfEntriesOfLedger avail(long... entryIds) {
        return new AvailabilityOfEntriesOfLedger(entryIds);
    }

    private static LedgerManager.LedgerRangeIterator rangesOf(LedgerManager.LedgerRange... ranges) {
        final Iterator<LedgerManager.LedgerRange> inner = Arrays.asList(ranges).iterator();
        return new LedgerManager.LedgerRangeIterator() {
            @Override
            public boolean hasNext() throws IOException {
                return inner.hasNext();
            }

            @Override
            public LedgerManager.LedgerRange next() throws IOException {
                return inner.next();
            }
        };
    }

    private LedgerMetadata closedMeta(long ledgerId, List<BookieId> ensemble) {
        // ensembleSize == writeQuorumSize == 3 so every entry is striped to every bookie;
        // ackQuorumSize == 1 keeps "least replicas" above AQ but below WQ for the missing entry.
        return LedgerMetadataBuilder.create()
                .withId(ledgerId)
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(1)
                .withClosedState()
                .withLastEntryId(0L)
                .withLength(100)
                .withDigestType(DigestType.CRC32)
                .withPassword(new byte[0])
                .newEnsembleEntry(0L, ensemble)
                .build();
    }

    private static <T> CompletableFuture<T> done(T value) {
        return CompletableFuture.completedFuture(value);
    }

    @Test
    public void testFindingInEarlierRangeIsNotWipedByLaterRange() throws Exception {
        final ServerConfiguration conf = new ServerConfiguration();

        final BookKeeperAdmin admin = mock(BookKeeperAdmin.class);
        final LedgerManager ledgerManager = mock(LedgerManager.class);
        final LedgerUnderreplicationManager urm = mock(LedgerUnderreplicationManager.class);

        when(urm.isLedgerReplicationEnabled()).thenReturn(true);
        when(urm.getLedgerUnreplicationInfo(anyLong())).thenReturn(null);

        // Two separate ledger ranges: range 1 = {A}, range 2 = {B}.
        when(ledgerManager.getLedgerRanges(anyLong())).thenReturn(rangesOf(
                new LedgerManager.LedgerRange(Collections.singleton(LEDGER_A)),
                new LedgerManager.LedgerRange(Collections.singleton(LEDGER_B))));

        final BookieId b0 = BookieId.parse("1.1.1.1:3181");
        final BookieId b1 = BookieId.parse("2.2.2.2:3181");
        final BookieId b2 = BookieId.parse("3.3.3.3:3181");
        final List<BookieId> ensemble = Arrays.asList(b0, b1, b2);

        when(ledgerManager.readLedgerMetadata(LEDGER_A))
                .thenReturn(done(new Versioned<>(closedMeta(LEDGER_A, ensemble), new LongVersion(1L))));
        when(ledgerManager.readLedgerMetadata(LEDGER_B))
                .thenReturn(done(new Versioned<>(closedMeta(LEDGER_B, ensemble), new LongVersion(1L))));

        // Ledger A: entry 0 is present on b0 and b1 but missing on b2 -> 2 of 3 replicas (>= AQ, < WQ).
        when(admin.asyncGetListOfEntriesOfLedger(b0, LEDGER_A)).thenReturn(done(avail(0L)));
        when(admin.asyncGetListOfEntriesOfLedger(b1, LEDGER_A)).thenReturn(done(avail(0L)));
        when(admin.asyncGetListOfEntriesOfLedger(b2, LEDGER_A)).thenReturn(done(avail()));

        // Ledger B: fully replicated, no findings.
        when(admin.asyncGetListOfEntriesOfLedger(b0, LEDGER_B)).thenReturn(done(avail(0L)));
        when(admin.asyncGetListOfEntriesOfLedger(b1, LEDGER_B)).thenReturn(done(avail(0L)));
        when(admin.asyncGetListOfEntriesOfLedger(b2, LEDGER_B)).thenReturn(done(avail(0L)));

        final TestStatsProvider statsProvider = new TestStatsProvider();
        final TestStatsProvider.TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        final AuditorStats auditorStats = new AuditorStats(statsLogger);

        final AuditorReplicasCheckTask task = new AuditorReplicasCheckTask(
                conf, auditorStats, admin, ledgerManager, urm,
                /* shutdownTaskHandler */ null,
                /* hasAuditCheckTask */ (flag, throwable) -> flag.set(false));

        task.runTask();

        assertEquals(
                "Ledger A (in the first range) has an entry below write-quorum; the later healthy "
                        + "range must not wipe that finding",
                1,
                auditorStats.getNumLedgersHavingLessThanWQReplicasOfAnEntryGuageValue().get());
    }
}
