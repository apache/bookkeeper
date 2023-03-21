/*
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
package org.apache.bookkeeper.benchmark;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A benchmark that benchmarks write throughput and latency.
 */
public class BenchThroughputLatency implements AddCallback, Runnable {
    static final Logger LOG = LoggerFactory.getLogger(BenchThroughputLatency.class);

    BookKeeper bk;
    LedgerHandle[] lh;
    AtomicLong counter;

    Semaphore sem;
    int numberOfLedgers = 1;
    final int sendLimit;
    final long[] latencies;

    static class Context {
        long localStartTime;
        long id;

        Context(long id, long time){
            this.id = id;
            this.localStartTime = time;
        }
    }

    public BenchThroughputLatency(int ensemble, int writeQuorumSize, int ackQuorumSize, byte[] passwd,
            int numberOfLedgers, int sendLimit, ClientConfiguration conf)
            throws BKException, IOException, InterruptedException {
        this.sem = new Semaphore(conf.getThrottleValue());
        bk = new BookKeeper(conf);
        this.counter = new AtomicLong(0);
        this.numberOfLedgers = numberOfLedgers;
        this.sendLimit = sendLimit;
        this.latencies = new long[sendLimit];
        try {
            lh = new LedgerHandle[this.numberOfLedgers];

            for (int i = 0; i < this.numberOfLedgers; i++) {
                lh[i] = bk.createLedger(ensemble, writeQuorumSize,
                                        ackQuorumSize,
                                        BookKeeper.DigestType.CRC32,
                                        passwd);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ledger Handle: " + lh[i].getId());
                }
            }
        } catch (BKException e) {
            e.printStackTrace();
        }
    }

    Random rand = new Random();
    public void close() throws InterruptedException, BKException {
        for (int i = 0; i < numberOfLedgers; i++) {
            if (lh[i] != null) {
                lh[i].close();
            }
        }
        bk.close();
    }

    long previous = 0;
    byte[] bytes;

    void setEntryData(byte[] data) {
        bytes = data;
    }

    int lastLedger = 0;
    private int getRandomLedger() {
         return rand.nextInt(numberOfLedgers);
    }

    int latencyIndex = -1;
    AtomicLong completedRequests = new AtomicLong(0);

    long duration = -1;
    public synchronized long getDuration() {
        return duration;
    }

    @Override
    public void run() {
        LOG.info("Running...");
        long start = previous = System.currentTimeMillis();

        int sent = 0;

        Thread reporter = new Thread() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            Thread.sleep(1000);
                            LOG.info("ms: {} req: {}", System.currentTimeMillis(), completedRequests.getAndSet(0));
                        }
                    } catch (InterruptedException ie) {
                        LOG.info("Caught interrupted exception, going away");
                        Thread.currentThread().interrupt();
                    }
                }
            };
        reporter.start();
        long beforeSend = System.nanoTime();

        while (!Thread.currentThread().isInterrupted() && sent < sendLimit) {
            try {
                sem.acquire();
                if (sent == 10000) {
                    long afterSend = System.nanoTime();
                    long time = afterSend - beforeSend;
                    LOG.info("Time to send first batch: {}s {}ns ", time / 1000 / 1000 / 1000, time);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            final int index = getRandomLedger();
            LedgerHandle h = lh[index];
            if (h == null) {
                LOG.error("Handle " + index + " is null!");
            } else {
                long nanoTime = System.nanoTime();
                lh[index].asyncAddEntry(bytes, this, new Context(sent, nanoTime));
                counter.incrementAndGet();
            }
            sent++;
        }
        LOG.info("Sent: "  + sent);
        try {
            int i = 0;
            while (this.counter.get() > 0) {
                Thread.sleep(1000);
                i++;
                if (i > 30) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting", e);
            Thread.currentThread().interrupt();
        }
        synchronized (this) {
            duration = System.currentTimeMillis() - start;
        }
        throughput = sent * 1000 / getDuration();

        reporter.interrupt();
        try {
            reporter.join();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        LOG.info("Finished processing in ms: " + getDuration() + " tp = " + throughput);
    }

    long throughput = -1;
    public long getThroughput() {
        return throughput;
    }

    long threshold = 20000;
    long runningAverageCounter = 0;
    long totalTime = 0;
    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        Context context = (Context) ctx;

        // we need to use the id passed in the context in the case of
        // multiple ledgers, and it works even with one ledger
        entryId = context.id;
        long newTime = System.nanoTime() - context.localStartTime;

        sem.release();
        counter.decrementAndGet();

        if (rc == 0) {
            latencies[(int) entryId] = newTime;
            completedRequests.incrementAndGet();
        }
    }

    @SuppressWarnings("deprecation")
    public static void main(String[] args)
            throws KeeperException, IOException, InterruptedException, ParseException, BKException {
        Options options = new Options();
        options.addOption("time", true, "Running time (seconds), default 60");
        options.addOption("entrysize", true, "Entry size (bytes), default 1024");
        options.addOption("ensemble", true, "Ensemble size, default 3");
        options.addOption("quorum", true, "Quorum size, default 2");
        options.addOption("ackQuorum", true, "Ack quorum size, default is same as quorum");
        options.addOption("throttle", true, "Max outstanding requests, default 10000");
        options.addOption("ledgers", true, "Number of ledgers, default 1");
        options.addOption("zookeeper", true, "Zookeeper ensemble, default \"localhost:2181\"");
        options.addOption("password", true, "Password used to create ledgers (default 'benchPasswd')");
        options.addOption("coordnode", true, "Coordination znode for multi client benchmarks (optional)");
        options.addOption("timeout", true, "Number of seconds after which to give up");
        options.addOption("sockettimeout", true, "Socket timeout for bookkeeper client. In seconds. Default 5");
        options.addOption("skipwarmup", false, "Skip warm up, default false");
        options.addOption("sendlimit", true, "Max number of entries to send. Default 20000000");
        options.addOption("latencyFile", true, "File to dump latencies. Default is latencyDump.dat");
        options.addOption("useV2", false, "Whether use V2 protocol to send requests to the bookie server.");
        options.addOption("warmupMessages", true, "Number of messages to warm up. Default 10000");
        options.addOption("help", false, "This message");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("BenchThroughputLatency <options>", options);
            System.exit(-1);
        }

        long runningTime = Long.parseLong(cmd.getOptionValue("time", "60"));
        String servers = cmd.getOptionValue("zookeeper", "localhost:2181");
        int entrysize = Integer.parseInt(cmd.getOptionValue("entrysize", "1024"));

        int ledgers = Integer.parseInt(cmd.getOptionValue("ledgers", "1"));
        int ensemble = Integer.parseInt(cmd.getOptionValue("ensemble", "3"));
        int quorum = Integer.parseInt(cmd.getOptionValue("quorum", "2"));
        int ackQuorum = quorum;
        if (cmd.hasOption("ackQuorum")) {
            ackQuorum = Integer.parseInt(cmd.getOptionValue("ackQuorum"));
        }
        int throttle = Integer.parseInt(cmd.getOptionValue("throttle", "10000"));
        int sendLimit = Integer.parseInt(cmd.getOptionValue("sendlimit", "20000000"));
        int warmupMessages = Integer.parseInt(cmd.getOptionValue("warmupMessages", "10000"));

        final int sockTimeout = Integer.parseInt(cmd.getOptionValue("sockettimeout", "5"));

        String coordinationZnode = cmd.getOptionValue("coordnode");
        final byte[] passwd = cmd.getOptionValue("password", "benchPasswd").getBytes(UTF_8);

        String latencyFile = cmd.getOptionValue("latencyFile", "latencyDump.dat");

        Timer timeouter = new Timer();
        if (cmd.hasOption("timeout")) {
            final long timeout = Long.parseLong(cmd.getOptionValue("timeout", "360")) * 1000;

            timeouter.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        System.err.println("Timing out benchmark after " + timeout + "ms");
                        System.exit(-1);
                    }
                }, timeout);
        }

        LOG.warn("(Parameters received) running time: " + runningTime
                + ", entry size: " + entrysize + ", ensemble size: " + ensemble
                + ", quorum size: " + quorum
                + ", throttle: " + throttle
                + ", number of ledgers: " + ledgers
                + ", zk servers: " + servers
                + ", latency file: " + latencyFile);

        long totalTime = runningTime * 1000;

        // Do a warmup run
        Thread thread;

        byte[] data = new byte[entrysize];
        Arrays.fill(data, (byte) 'x');

        ClientConfiguration conf = new ClientConfiguration();
        conf.setThrottleValue(throttle).setReadTimeout(sockTimeout).setZkServers(servers);

        if (cmd.hasOption("useV2")) {
            conf.setUseV2WireProtocol(true);
        }

        if (!cmd.hasOption("skipwarmup")) {
            long throughput;
            LOG.info("Starting warmup");

            throughput = warmUp(data, ledgers, ensemble, quorum, passwd, warmupMessages, conf);
            LOG.info("Warmup tp: " + throughput);
            LOG.info("Warmup phase finished");
        }


        // Now do the benchmark
        BenchThroughputLatency bench = new BenchThroughputLatency(ensemble, quorum, ackQuorum,
                passwd, ledgers, sendLimit, conf);
        bench.setEntryData(data);
        thread = new Thread(bench);
        ZooKeeper zk = null;

        if (coordinationZnode != null) {
            final CountDownLatch connectLatch = new CountDownLatch(1);
            zk = new ZooKeeper(servers, 15000, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getState() == KeeperState.SyncConnected) {
                            connectLatch.countDown();
                        }
                    }});
            if (!connectLatch.await(10, TimeUnit.SECONDS)) {
                LOG.error("Couldn't connect to zookeeper at " + servers);
                zk.close();
                System.exit(-1);
            }

            final CountDownLatch latch = new CountDownLatch(1);
            LOG.info("Waiting for " + coordinationZnode);
            if (zk.exists(coordinationZnode, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == EventType.NodeCreated) {
                        latch.countDown();
                    }
                }}) != null) {
                latch.countDown();
            }
            latch.await();
            LOG.info("Coordination znode created");
        }
        thread.start();
        Thread.sleep(totalTime);
        thread.interrupt();
        thread.join();

        LOG.info("Calculating percentiles");

        int numlat = 0;
        for (int i = 0; i < bench.latencies.length; i++) {
            if (bench.latencies[i] > 0) {
                numlat++;
            }
        }
        int numcompletions = numlat;
        numlat = Math.min(bench.sendLimit, numlat);
        long[] latency = new long[numlat];
        int j = 0;
        for (int i = 0; i < bench.latencies.length && j < numlat; i++) {
            if (bench.latencies[i] > 0) {
                latency[j++] = bench.latencies[i];
            }
        }
        Arrays.sort(latency);

        long tp = (long) ((double) (numcompletions * 1000.0) / (double) bench.getDuration());

        LOG.info(numcompletions + " completions in " + bench.getDuration() + " milliseconds: " + tp + " ops/sec");

        if (zk != null) {
            zk.create(coordinationZnode + "/worker-",
                      ("tp " + tp + " duration " + bench.getDuration()).getBytes(UTF_8),
                      ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            zk.close();
        }

        // dump the latencies for later debugging (it will be sorted by entryid)
        OutputStream fos = new BufferedOutputStream(new FileOutputStream(latencyFile));

        for (Long l: latency) {
            fos.write((l + "\t" + (l / 1000000) + "ms\n").getBytes(UTF_8));
        }
        fos.flush();
        fos.close();

        // now get the latencies
        LOG.info("99th percentile latency: {}", percentile(latency, 99));
        LOG.info("95th percentile latency: {}", percentile(latency, 95));

        bench.close();
        timeouter.cancel();
    }

    private static double percentile(long[] latency, int percentile) {
        int size = latency.length;
        double percent = (double) percentile / 100;
        int sampleSize = (int) (size * percent);
        long total = 0;
        int count = 0;
        for (int i = 0; i < sampleSize; i++) {
            total += latency[i];
            count++;
        }
        return ((double) total / (double) count) / 1000000.0;
    }

    /**
     * The benchmark is assuming zookeeper based metadata service.
     *
     * <p>TODO: update benchmark to use metadata service uri {@link https://github.com/apache/bookkeeper/issues/1331}
     */
    private static long warmUp(byte[] data, int ledgers, int ensemble, int qSize,
                               byte[] passwd, int warmupMessages, ClientConfiguration conf)
            throws KeeperException, IOException, InterruptedException, BKException {
        final CountDownLatch connectLatch = new CountDownLatch(1);
        final int bookies;
        String bookieRegistrationPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf) + "/" + AVAILABLE_NODE;
        ZooKeeper zk = null;
        try {
            final String servers = ZKMetadataDriverBase.resolveZkServers(conf);
            zk = new ZooKeeper(servers, 15000, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getState() == KeeperState.SyncConnected) {
                            connectLatch.countDown();
                        }
                    }});
            if (!connectLatch.await(10, TimeUnit.SECONDS)) {
                LOG.error("Couldn't connect to zookeeper at " + servers);
                throw new IOException("Couldn't connect to zookeeper " + servers);
            }
            bookies = zk.getChildren(bookieRegistrationPath, false).size() - 1;
        } finally {
            if (zk != null) {
                zk.close();
            }
        }

        BenchThroughputLatency warmup = new BenchThroughputLatency(bookies, bookies, bookies, passwd,
                                                                   ledgers, warmupMessages, conf);
        warmup.setEntryData(data);
        Thread thread = new Thread(warmup);
        thread.start();
        thread.join();
        warmup.close();
        return warmup.getThroughput();
    }
}
