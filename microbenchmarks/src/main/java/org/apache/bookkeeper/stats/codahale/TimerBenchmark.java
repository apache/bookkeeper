/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.stats.codahale;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Microbenchmarks for default and improved (fast) Codahale timers.
 * To run:
 * build project from command line (mvn clean install).
 * execute ./run.sh
 * Specify argument "TimerBenchmark" to only run this benchmark.
 */
public class TimerBenchmark {

    /**
     * Type of Timer.
     */
    public enum TimerType {
        CodahaleTimer,
        FastTimer
    }

    /**
     * Thread-local state.
     */
    @State(Scope.Thread)
    public static class MyState {

        @Param
        public TimerType timerType;

        @Param({"1", "10", "100"})
        public int timersPerThread;

        private static OpStatsLogger[] timers;
        private int timerIdx = 0;

        private static long[] times;
        private int timeIdx = 0;

        @Setup(Level.Trial)
        public void doSetup() throws Exception {
            StatsLogger logger = null;
            switch (timerType) {
            case CodahaleTimer:
                logger = new CodahaleMetricsProvider().getStatsLogger("test");
                break;
            case FastTimer:
                logger = new FastCodahaleMetricsProvider().getStatsLogger("test");
                break;
            }

            synchronized (MyState.class) {
                // timers (and response times) are shared across threads to test
                // concurrency of timer updates.
                if (timers == null) {
                    timers = new OpStatsLogger[timersPerThread];
                    for (int i = 0; i < timersPerThread; i++) {
                        timers[i] = logger.getOpStatsLogger("test-timer-" + i);
                    }

                    // just a bunch of random response times to not always hit the same bucket
                    times = new long[1000];
                    for (int i = 0; i < times.length; i++) {
                        times[i] = Math.abs(ThreadLocalRandom.current().nextLong() % 1000);
                    }
                }
            }
        }

        public OpStatsLogger getTimer() {
            return timers[(timerIdx++) % timers.length];
        }

        public long getTime() {
            return times[(timeIdx++) % times.length];
        }

        public boolean isGetSnapshot() {
            // create a snapshot every 109 operations (typically snapshot creations will be much more infrequent)
            // 109 is prime, guaranteeing that we will create snapshots across all timers
            if (timeIdx % 109 == 0) {
                timeIdx++;
                return true;
            } else {
                return false;
            }
        }

    }

    /**
     * Tests the performance of (concurrent) timer updates.
     * Note that test duration must exceed TIME_WINDOW (default: 60) to trigger
     * FastTimer's housekeeping. Manual tests show little performance difference
     * for longer running tests (since housekeeping is infrequent and cheap), so
     * we keep the test duration low to not have tests run for too long.
     * @param state
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(4)
    @Fork(value = 1, warmups = 1)
    public void timerTest(MyState state) {
        state.getTimer().registerSuccessfulEvent(state.getTime(), TimeUnit.MILLISECONDS);
    }

    /**
     * Tests the performance of (concurrent) timer updates with
     * the creation of snapshots. We expect snapshot creation to
     * be infrequent (e.g. once every N seconds), while timer updates
     * are frequent (for many timers hundreds or thousands of times
     * per second). Here we're testing the creation of snapshots at
     * a rate much higher than we would expect in real life.
     * @param state
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
    @Threads(4)
    @Fork(value = 1, warmups = 1)
    public void timerTestWithSnapshots(MyState state) {
        OpStatsLogger timer = state.getTimer();
        if (state.isGetSnapshot()) {
            timer.toOpStatsData();
        } else {
            timer.registerSuccessfulEvent(state.getTime(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Test routing for manual testing of memory footprint of default Codahale Timer vs. improved FastTimer.
     * JMH can't do that, so we have a small stand-alone test routine here.
     * Run with: java -Xmx1g -cp target/benchmarks.jar org.apache.bookkeeper.stats.codahale.TimerBenchmark <codahale|fast>
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 1 ||
                (!args[0].equalsIgnoreCase("codahale") && !args[0].equalsIgnoreCase("fast"))) {
            System.out.println("usage: " + TimerBenchmark.class.getCanonicalName() + " <codahale|fast>");
            System.exit(1);
        }
        StatsLogger logger = null;
        if (args[0].equalsIgnoreCase("codahale")) {
            logger = new CodahaleMetricsProvider().getStatsLogger("test");
        } else {
            logger = new FastCodahaleMetricsProvider().getStatsLogger("test");
        }
        System.out.println("Using " + logger.getClass().getCanonicalName());
        System.out.println("Creating 1000 OpStatsLoggers (2000 Timers) and updating each of them 1000 times ...");
        OpStatsLogger[] timers = new OpStatsLogger[1000];
        for (int i=0; i<timers.length; i++) {
            timers[i] = logger.getOpStatsLogger("test-timer-" + i);
        }
        long[] times = new long[199]; // 199 is prime, so each timer will get each time
        for (int i = 0; i < times.length; i++) {
            times[i] = Math.abs(ThreadLocalRandom.current().nextLong() % 1000);
        }
        for (int i=0; i<1000 * timers.length; i++) {
            timers[i % timers.length].registerSuccessfulEvent(times[i % times.length], TimeUnit.MILLISECONDS);
            timers[i % timers.length].registerFailedEvent(times[i % times.length], TimeUnit.MILLISECONDS);
        }
        times = null; // let it become garbage
        System.out.println("Done.");
        System.out.println("Now run 'jmap -histo:live <pid>' on this JVM to get a heap histogram, then kill this JVM.");
        while(true) {
            try {
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch(Exception e) {
                // ignore
            }
        }
    }

}
