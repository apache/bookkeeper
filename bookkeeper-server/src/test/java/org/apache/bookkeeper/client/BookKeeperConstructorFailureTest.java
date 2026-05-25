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
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that the {@link BookKeeper} client constructor releases every
 * resource it has already allocated when initialization fails part-way
 * through (e.g. the metadata service URI is invalid or unreachable).
 *
 * <p>Before the fix, a failing {@code BookKeeper.forConfig(conf).build()}
 * leaked the worker pools it had just created
 * ({@code BookKeeperClientScheduler}, {@code BookKeeperClientWorker-*},
 * {@code BookKeeperHighPriorityThread}, and
 * {@code BKClientMetaDataPollScheduler-*} when disk-weight is enabled),
 * along with the underlying bookie / metadata sockets. Repeatedly catching
 * the construction exception in user code therefore leaked threads
 * monotonically and eventually exhausted the JVM.
 *
 * <p>The test does not need a real bookie cluster: it deliberately points the
 * client at an invalid metadata service URI so initialization fails <i>after</i>
 * the worker pools have been allocated but <i>before</i> the metadata driver
 * is up. That is the exact failure path which used to leak resources.
 *
 * <p><b>Why this test counts threads by prefix instead of diffing name sets:</b>
 * {@link org.apache.bookkeeper.common.util.OrderedExecutor} names its threads
 * {@code <baseName>-OrderedScheduler-<i>-<n>}, and the {@code <n>} counter
 * resets every time a new pool is built. Two separate failed {@code build()}
 * calls therefore produce threads with <i>identical</i> names. A
 * {@code Set<String>} would silently dedupe them and miss the leak; a
 * per-prefix count does not.
 */
public class BookKeeperConstructorFailureTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(BookKeeperConstructorFailureTest.class);

    /**
     * An invalid metadata service URI scheme. {@code MetadataDrivers#getClientDriver(URI)}
     * fails to resolve any driver for it and throws, exercising the failure path
     * <i>after</i> the worker pools / bookie client have been created but
     * <i>before</i> the metadata client driver is initialized — the original
     * leak path.
     */
    private static final String INVALID_METADATA_URI = "zk+null://127.0.0.1:1/ledgers";

    /**
     * Thread-name prefixes the BookKeeper client constructor creates. If
     * {@code close()} runs correctly on the failure path, none of these may
     * outlive the failed construction.
     */
    private static final String[] CLIENT_THREAD_PREFIXES = new String[] {
        "BookKeeperClientScheduler",
        "BookKeeperClientWorker",
        "BookKeeperHighPriorityThread",
        "BKClientMetaDataPollScheduler",
    };

    /**
     * A single failing {@code build()} must not leak any client-owned thread.
     */
    @Test
    public void testNoThreadLeakWhenMetadataServiceUnavailable() throws Exception {
        Map<String, Integer> baseline = snapshotClientThreadCounts();
        LOG.info("baseline thread counts: {}", baseline);

        ClientConfiguration conf = newFailingClientConf();
        try {
            BookKeeper.forConfig(conf).build();
            fail("BookKeeper construction should have failed");
        } catch (Exception expected) {
            LOG.info("expected failure during BookKeeper construction: {}", expected.toString());
        }

        waitForThreadShutdown(baseline, 10_000L);

        Map<String, Integer> leak = computeLeak(baseline, snapshotClientThreadCounts());
        assertTrue("BookKeeper constructor leaked threads after failure: " + leak,
                leak.isEmpty());
    }

    /**
     * Repeated failing {@code build()} calls must not accumulate threads.
     * Without the fix, each iteration leaked ~3 threads, so even a small
     * loop is enough to make the leak unambiguous.
     */
    @Test
    public void testRepeatedFailedBuildsDoNotAccumulateThreads() throws Exception {
        Map<String, Integer> baseline = snapshotClientThreadCounts();
        LOG.info("baseline thread counts: {}", baseline);

        ClientConfiguration conf = newFailingClientConf();
        final int iterations = 20;
        for (int i = 0; i < iterations; i++) {
            try {
                BookKeeper.forConfig(conf).build();
                fail("BookKeeper construction should have failed at iteration " + i);
            } catch (Exception expected) {
                LOG.debug("iteration {} failed as expected: {}", i, expected.toString());
            }
        }

        waitForThreadShutdown(baseline, 30_000L);

        Map<String, Integer> current = snapshotClientThreadCounts();
        Map<String, Integer> leak = computeLeak(baseline, current);
        if (!leak.isEmpty()) {
            LOG.error("Thread leak detected after {} failed build()s. baseline={}, current={}, leak={}",
                    iterations, baseline, current, leak);
            fail("BookKeeper constructor leaked threads after " + iterations
                    + " failed build()s. baseline=" + baseline
                    + ", current=" + current
                    + ", leak=" + leak);
        }
    }

    // ---------- helpers ----------

    private static ClientConfiguration newFailingClientConf() {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(INVALID_METADATA_URI);
        conf.setZkTimeout(1000);
        conf.setClientConnectTimeoutMillis(1000);
        return conf;
    }

    /**
     * Returns a count of currently-live threads, grouped by the
     * {@link #CLIENT_THREAD_PREFIXES} entry they originated from.
     *
     * <p>Counting (not name-set diffing) is required because OrderedExecutor
     * resets its per-pool name counter every {@code build()}, so two separate
     * leaked schedulers can share an identical thread name.
     */
    private static Map<String, Integer> snapshotClientThreadCounts() {
        ThreadMXBean mx = ManagementFactory.getThreadMXBean();
        ThreadInfo[] infos = mx.getThreadInfo(mx.getAllThreadIds(), 0);
        Map<String, Integer> counts = new TreeMap<>();
        for (String prefix : CLIENT_THREAD_PREFIXES) {
            counts.put(prefix, 0);
        }
        for (ThreadInfo info : infos) {
            if (info == null) {
                continue;
            }
            String prefix = matchPrefix(info.getThreadName());
            if (prefix != null) {
                counts.merge(prefix, 1, Integer::sum);
            }
        }
        return counts;
    }

    /**
     * Returns the {@link #CLIENT_THREAD_PREFIXES} entry the given thread name
     * starts with, or {@code null} if it is not a BookKeeper-client thread.
     */
    private static String matchPrefix(String name) {
        if (name == null) {
            return null;
        }
        return Arrays.stream(CLIENT_THREAD_PREFIXES)
                .filter(name::startsWith)
                .findFirst()
                .orElse(null);
    }

    /**
     * Computes per-prefix leakage: {@code current[prefix] - baseline[prefix]}
     * for any prefix where the result is positive. An empty map means no leak.
     */
    private static Map<String, Integer> computeLeak(Map<String, Integer> baseline,
                                                    Map<String, Integer> current) {
        Map<String, Integer> leak = new LinkedHashMap<>();
        for (String prefix : CLIENT_THREAD_PREFIXES) {
            int delta = current.getOrDefault(prefix, 0) - baseline.getOrDefault(prefix, 0);
            if (delta > 0) {
                leak.put(prefix, delta);
            }
        }
        return leak;
    }

    /**
     * Polls per-prefix live thread counts, waiting until none exceeds the
     * baseline, or until {@code timeoutMs} elapses.
     */
    private static void waitForThreadShutdown(Map<String, Integer> baseline, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (computeLeak(baseline, snapshotClientThreadCounts()).isEmpty()) {
                return;
            }
            Thread.sleep(100);
        }
    }
}
