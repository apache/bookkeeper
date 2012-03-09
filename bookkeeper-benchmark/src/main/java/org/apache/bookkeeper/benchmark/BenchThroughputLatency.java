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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

public class BenchThroughputLatency implements AddCallback, Runnable {
    static Logger LOG = LoggerFactory.getLogger(BenchThroughputLatency.class);

    BookKeeper bk;
    LedgerHandle lh[];
    AtomicLong counter;

    Semaphore sem;
    int pace;
    int throttle;
    int numberOfLedgers = 1;
    final String servers;

    class Context {
        long localStartTime;
        long globalStartTime;
        long id;

        Context(long id, long time){
            this.id = id;
            this.localStartTime = this.globalStartTime = time;
        }
    }

    public BenchThroughputLatency(int ensemble, int qSize, byte[] passwd,
                                  int throttle, int numberOfLedgers, String servers) 
            throws KeeperException, IOException, InterruptedException {
        this.sem = new Semaphore(throttle);
        this.pace = pace;
        this.throttle = throttle;

        ClientConfiguration conf = new ClientConfiguration();
        conf.setThrottleValue(100000);
        conf.setZkServers(servers);
        this.servers = servers;

        bk = new BookKeeper(conf);
        this.counter = new AtomicLong(0);
        this.numberOfLedgers = numberOfLedgers;
        try{
            lh = new LedgerHandle[this.numberOfLedgers];

            for(int i = 0; i < this.numberOfLedgers; i++) {
                lh[i] = bk.createLedger(ensemble, qSize, BookKeeper.DigestType.CRC32, 
                                        passwd);
                LOG.info("Ledger Handle: " + lh[i].getId());
            }
        } catch (BKException e) {
            e.printStackTrace();
        }
    }

    Random rand = new Random();
    public void close() throws InterruptedException, BKException {
        for(int i = 0; i < numberOfLedgers; i++) {
            lh[i].close();
        }
        bk.close();
    }

    long previous = 0;
    byte bytes[];

    void setEntryData(byte data[]) {
        bytes = data;
    }

    int lastLedger = 0;
    private int getRandomLedger() {
         return rand.nextInt(numberOfLedgers);
    }

    int sendLimit = 2000000;
    long latencies[] = new long[sendLimit];
    int latencyIndex = -1;
    AtomicLong completedRequests = new AtomicLong(0);

    public void setSendLimit(int sendLimit) {
        this.sendLimit = sendLimit;
        latencies = new long[sendLimit];
    }

    long duration = -1;
    synchronized public long getDuration() {
        return duration;
    }

    public void run() {
        LOG.info("Running...");
        long start = previous = System.currentTimeMillis();

        byte messageCount = 0;
        int sent = 0;

        Thread reporter = new Thread() {
                public void run() {
                    try {
                        while(true) {
                            Thread.sleep(200);
                            LOG.info("ms: {} req: {}", System.currentTimeMillis(), completedRequests.getAndSet(0));
                        }
                    } catch (InterruptedException ie) {
                        LOG.info("Caught interrupted exception, going away");
                    }
                }
            };
        reporter.start();
        long beforeSend = System.nanoTime();

        while(!Thread.currentThread().isInterrupted() && sent < sendLimit) {
            try {
                sem.acquire();
                if (sent == 10000) {
                    long afterSend = System.nanoTime();
                    long time = afterSend - beforeSend;
                    LOG.info("Time to send first batch: {}s {}ns ",
                             time/1000/1000/1000, time);
                }
            } catch (InterruptedException e) {
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
            synchronized (this) {
                while(this.counter.get() > 0)
                    Thread.sleep(1000);
            }
        } catch(InterruptedException e) {
            LOG.error("Interrupted while waiting", e);
        }
        synchronized(this) {
            duration = System.currentTimeMillis() - start;
        }
        throughput = sent*1000/duration;

        reporter.interrupt();
        try {
            reporter.join();
        } catch (InterruptedException ie) {
            // ignore
        }
        LOG.info("Finished processing in ms: " + duration + " tp = " + throughput);
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

        latencies[(int)entryId] = newTime;

        completedRequests.incrementAndGet();
    }

    public static void main(String[] args)
            throws KeeperException, IOException, InterruptedException, ParseException, BKException {
        Options options = new Options();
        options.addOption("time", true, "Running time (seconds), default 60");
        options.addOption("entrysize", true, "Entry size (bytes), default 1024");
        options.addOption("ensemble", true, "Ensemble size, default 3");
        options.addOption("quorum", true, "Quorum size, default 2");
        options.addOption("throttle", true, "Max outstanding requests, default 10000");
        options.addOption("ledgers", true, "Number of ledgers, default 1");
        options.addOption("zookeeper", true, "Zookeeper ensemble, default \"localhost:2181\"");
        options.addOption("password", true, "Password used to create ledgers (default 'benchPasswd')");
        options.addOption("coord_node", true, "Coordination znode for multi client benchmarks (optional)");
        options.addOption("help", false, "This message");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("BenchThroughputLatency <options>", options);
            System.exit(-1);
        }

        long runningTime = Long.valueOf(cmd.getOptionValue("time", "60"));
        String servers = cmd.getOptionValue("zookeeper", "localhost:2181");
        int entrysize = Integer.valueOf(cmd.getOptionValue("entrysize", "1024"));

        int ledgers = Integer.valueOf(cmd.getOptionValue("ledgers", "1"));
        int ensemble = Integer.valueOf(cmd.getOptionValue("ensemble", "3"));
        int quorum = Integer.valueOf(cmd.getOptionValue("quorum", "2"));
        int throttle = Integer.valueOf(cmd.getOptionValue("throttle", "10000"));

        String coordinationZnode = cmd.getOptionValue("coord_node");
        final byte[] passwd = cmd.getOptionValue("password", "benchPasswd").getBytes();

        LOG.warn("(Parameters received) running time: " + runningTime +
                ", entry size: " + entrysize + ", ensemble size: " + ensemble +
                ", quorum size: " + quorum +
                ", throttle: " + throttle +
                ", number of ledgers: " + ledgers +
                ", zk servers: " + servers);

        long totalTime = runningTime*1000;

        // Do a warmup run
        Thread thread;

        long lastWarmUpTP = -1;
        long throughput;
        LOG.info("Starting warmup");
        byte data[] = new byte[entrysize];
        Arrays.fill(data, (byte)'x');

        while(lastWarmUpTP < (throughput = warmUp(servers, data, ledgers, ensemble, quorum, passwd, throttle))) {
            LOG.info("Warmup tp: " + throughput);
            lastWarmUpTP = throughput;
            // we will just run once, so lets break
            break;
        }
        LOG.info("Warmup phase finished");

        // Now do the benchmark
        BenchThroughputLatency bench = new BenchThroughputLatency(ensemble, quorum, passwd, throttle, ledgers, servers);
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
        ArrayList<Long> latency = new ArrayList<Long>();
        for(int i = 0; i < bench.latencies.length; i++) {
            if (bench.latencies[i] > 0) {
                latency.add(bench.latencies[i]);
            }
        }
        double tp = (double)latency.size()*1000.0/(double)bench.getDuration();
        LOG.info(latency.size() + " completions in " + bench.getDuration() + " seconds: " + tp + " ops/sec");

        if (zk != null) {
            zk.create(coordinationZnode + "/worker-", 
                      ("tp " + tp + " duration " + bench.getDuration()).getBytes(), 
                      ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            zk.close();
        }

        // dump the latencies for later debugging (it will be sorted by entryid)
        OutputStream fos = new BufferedOutputStream(new FileOutputStream("latencyDump.dat"));

        for(Long l: latency) {
            fos.write((Long.toString(l)+"\t"+(l/1000000)+ "ms\n").getBytes());
        }
        fos.flush();
        fos.close();

        // now get the latencies
        Collections.sort(latency);
        LOG.info("99th percentile latency: {}", percentile(latency, 99));
        LOG.info("95th percentile latency: {}", percentile(latency, 95));

        bench.close();
    }

    private static double percentile(ArrayList<Long> latency, int percentile) {
        int size = latency.size();
        int sampleSize = (size * percentile) / 100;
        long total = 0;
        int count = 0;
        for(int i = 0; i < sampleSize; i++) {
            total += latency.get(i);
            count++;
        }
        return ((double)total/(double)count)/1000000.0;
    }

    private static long warmUp(String servers, byte[] data,
                               int ledgers, int ensemble, int qSize, byte[] passwd, int throttle)
            throws KeeperException, IOException, InterruptedException, BKException {
        final CountDownLatch connectLatch = new CountDownLatch(1);
        final int bookies;
        ZooKeeper zk = null;
        try {
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
            bookies = zk.getChildren("/ledgers/available", false).size();
        } finally {
            if (zk != null) {
                zk.close();
            }
        }

        BenchThroughputLatency warmup = new BenchThroughputLatency(bookies, bookies, passwd,
                                                                   throttle, ledgers, servers);
        int limit = 50000;

        warmup.setSendLimit(limit);
        warmup.setEntryData(data);
        Thread thread = new Thread(warmup);
        thread.start();
        thread.join();
        warmup.close();
        return warmup.getThroughput();
    }
}
