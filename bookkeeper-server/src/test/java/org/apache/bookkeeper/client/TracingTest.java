/*
 *
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
 *
 */

package org.apache.bookkeeper.client;

import static org.junit.Assert.assertTrue;

import io.opentracing.Scope;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.GlobalTracer;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test tracing.
 */
@Slf4j
@SuppressWarnings("deprecation")
public class TracingTest extends BookKeeperClusterTestCase {
    final byte[] entry = "Test Entry".getBytes();

    BookKeeper bkc;
    LedgerHandle lh;

    final MockTracer mockTracer;

    public TracingTest() {
        super(3);
        baseConf.setNumAddWorkerThreads(0);
        baseConf.setNumReadWorkerThreads(0);
        baseConf.setJournalAdaptiveGroupWrites(false);
        baseConf.setJournalMaxGroupWaitMSec(0);
        baseConf.setPreserveMdcForTaskExecution(true);
        baseConf.setReadOnlyModeEnabled(true);

        // for read-only bookie
        baseConf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        baseConf.setEntryLogFilePreAllocationEnabled(false);
        baseConf.setMinUsableSizeForEntryLogCreation(Long.MAX_VALUE);

        mockTracer = new MockTracer();
        GlobalTracer.register(mockTracer);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ClientConfiguration conf = new ClientConfiguration();
        conf.setReadTimeout(360)
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri())
                .setPreserveMdcForTaskExecution(true);

        bkc = new BookKeeper(conf);
        log.info("creating ledger");
        lh = bkc.createLedgerAdv(3, 3, 3, BookKeeper.DigestType.CRC32, new byte[]{});
        mockTracer.reset();
    }

    @After
    public void tearDown() throws Exception {
        lh.close();
        bkc.close();

        super.tearDown();
    }

    @Test
    public void testSimpleAdd() throws Exception {
        try (Scope ignored = GlobalTracer.get()
                    .buildSpan("test-add").startActive(true)) {
            lh.addEntry(0, entry);
        }

        List<MockSpan> spans = mockTracer.finishedSpans();
        Set<String> operations = spans.stream()
                .map(x -> x.operationName().toLowerCase())
                .collect(Collectors.toSet());

        assertTrue("addEntryAsync", operations.contains("addentryasync"));
        assertTrue("write-storage", operations.contains("write-storage"));
        assertTrue("write-journal", operations.contains("write-journal"));
        assertTrue("addEntryAsync", operations.contains("bookkeeperclientworker-enqueued"));
    }
}
