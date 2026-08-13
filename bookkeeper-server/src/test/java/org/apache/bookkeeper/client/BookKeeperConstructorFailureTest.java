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

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that when {@link BookKeeper} construction fails at metadata-driver
 * initialization, the {@link org.apache.bookkeeper.common.util.OrderedExecutor}
 * pools allocated before that point are released and do not accumulate across
 * repeated failing {@code build()} calls.
 *
 * <p>This test targets the <em>early-failure</em> path (an unreachable
 * ZooKeeper URI), which is the most common production misconfiguration and the
 * historical source of thread leaks. It does not cover resources allocated after
 * the metadata driver (e.g. {@code eventLoopGroup}, {@code requestTimer},
 * {@code bookieInfoScheduler}); those code paths require a reachable ZK and are
 * exercised by {@link BookKeeperCloseTest} together with the construction
 * cleanup logic in {@link BookKeeper}.
 *
 * <p>Every {@code build()} attempt runs inside a dedicated {@link ThreadGroup}
 * and only threads living in that group are counted, so concurrently executing
 * tests in the same JVM cannot pollute the result.
 */
public class BookKeeperConstructorFailureTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(BookKeeperConstructorFailureTest.class);

    private static final String UNREACHABLE_METADATA_URI = "zk://127.0.0.1:1/ledgers";

    /**
     * Thread-name prefixes of the three OrderedExecutor pools that BookKeeper allocates
     * <em>before</em> {@code metadataDriver.initialize(...)}. With an unreachable ZK URI,
     * construction fails inside that call, so only these three pools can possibly leak
     * on this code path.
     */
    private static final String[] CLIENT_THREAD_PREFIXES = new String[] {
        "BookKeeperClientScheduler",
        "BookKeeperClientWorker",
        "BookKeeperHighPriorityThread",
    };

    private static final int FAILED_BUILD_ITERATIONS = 5;

    private static final long THREAD_SHUTDOWN_TIMEOUT_MS = 5_000L;

    /** Repeated failing {@code build()} calls must not accumulate worker-pool threads. */
    @Test
    public void testRepeatedFailedBuildsDoNotAccumulateThreads() throws Exception {
        ThreadGroup group = new ThreadGroup("bk-ctor-failure-test");
        runFailingBuildsIn(group);
        waitForThreadShutdown(group, THREAD_SHUTDOWN_TIMEOUT_MS);

        Map<String, Integer> leak = snapshotClientThreadCounts(group);
        if (hasLeak(leak)) {
            LOG.error("Thread leak detected after {} failed build()s in group {}: {}",
                    FAILED_BUILD_ITERATIONS, group.getName(), leak);
            fail("BookKeeper constructor leaked threads after " + FAILED_BUILD_ITERATIONS
                    + " failed build()s in group " + group.getName() + ": " + leak);
        }
    }

    private static void runFailingBuildsIn(ThreadGroup group) throws InterruptedException {
        ClientConfiguration conf = newFailingClientConf();
        Throwable[] driverError = new Throwable[1];
        Thread driver = new Thread(group, () -> {
            for (int i = 0; i < FAILED_BUILD_ITERATIONS; i++) {
                try {
                    BookKeeper.forConfig(conf).build();
                    driverError[0] = new AssertionError(
                            "BookKeeper construction should have failed at iteration " + i);
                    return;
                } catch (Exception expected) {
                    LOG.debug("iteration {} failed as expected: {}", i, expected.toString());
                }
            }
        }, "bk-ctor-failure-driver");
        driver.start();
        driver.join();
        if (driverError[0] instanceof AssertionError) {
            throw (AssertionError) driverError[0];
        }
    }

    // ---------- helpers ----------

    private static ClientConfiguration newFailingClientConf() {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(UNREACHABLE_METADATA_URI);
        conf.setZkTimeout(1000);
        conf.setZkRetryBackoffMaxRetries(0);
        return conf;
    }

    /** Returns the live-thread counts inside {@code group} grouped by {@link #CLIENT_THREAD_PREFIXES}. */
    private static Map<String, Integer> snapshotClientThreadCounts(ThreadGroup group) {
        Thread[] threads = new Thread[Math.max(16, group.activeCount() * 2)];
        int n;
        // Loop to handle the race where a thread starts between activeCount() and enumerate().
        while ((n = group.enumerate(threads, true)) == threads.length) {
            threads = new Thread[threads.length * 2];
        }
        Map<String, Integer> counts = new TreeMap<>();
        for (String prefix : CLIENT_THREAD_PREFIXES) {
            counts.put(prefix, 0);
        }
        for (int i = 0; i < n; i++) {
            Thread t = threads[i];
            if (t == null || !t.isAlive()) {
                continue;
            }
            String prefix = matchPrefix(t.getName());
            if (prefix != null) {
                counts.merge(prefix, 1, Integer::sum);
            }
        }
        return counts;
    }

    private static String matchPrefix(String name) {
        if (name == null) {
            return null;
        }
        return Arrays.stream(CLIENT_THREAD_PREFIXES)
                .filter(name::startsWith)
                .findFirst()
                .orElse(null);
    }

    private static boolean hasLeak(Map<String, Integer> counts) {
        for (Integer v : counts.values()) {
            if (v != null && v > 0) {
                return true;
            }
        }
        return false;
    }

    private static void waitForThreadShutdown(ThreadGroup group, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (!hasLeak(snapshotClientThreadCounts(group))) {
                return;
            }
            Thread.sleep(50);
        }
    }
}