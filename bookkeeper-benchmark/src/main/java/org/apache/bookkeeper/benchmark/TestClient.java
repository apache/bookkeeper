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
package org.apache.bookkeeper.benchmark;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple test program to compare the performance of writing to
 * BookKeeper and to the local file system.
 */
public class TestClient {
    private static final Logger LOG = LoggerFactory.getLogger(TestClient.class);

    /**
     * First says if entries should be written to BookKeeper (0) or to the local
     * disk (1). Second parameter is an integer defining the length of a ledger entry.
     * Third parameter is the number of writes.
     *
     * @param args
     */
    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("length", true, "Length of packets being written. Default 1024");
        options.addOption("target", true, "Target medium to write to. Options are bk, fs & hdfs. Default fs");
        options.addOption("runfor", true, "Number of seconds to run for. Default 60");
        options.addOption("path", true, "Path to write to. fs & hdfs only. Default /foobar");
        options.addOption("zkservers", true, "ZooKeeper servers, comma separated. bk only. Default localhost:2181.");
        options.addOption("bkensemble", true, "BookKeeper ledger ensemble size. bk only. Default 3");
        options.addOption("bkquorum", true, "BookKeeper ledger quorum size. bk only. Default 2");
        options.addOption("bkthrottle", true, "BookKeeper throttle size. bk only. Default 10000");
        options.addOption("sync", false, "Use synchronous writes with BookKeeper. bk only.");
        options.addOption("numconcurrent", true, "Number of concurrently clients. Default 1");
        options.addOption("timeout", true, "Number of seconds after which to give up");
        options.addOption("help", false, "This message");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("TestClient <options>", options);
            System.exit(-1);
        }

        int length = Integer.parseInt(cmd.getOptionValue("length", "1024"));
        String target = cmd.getOptionValue("target", "fs");
        long runfor = Long.parseLong(cmd.getOptionValue("runfor", "60")) * 1000;

        StringBuilder sb = new StringBuilder();
        while (length-- > 0) {
            sb.append('a');
        }

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

        BookKeeper bkc = null;
        try {
            int numFiles = Integer.parseInt(cmd.getOptionValue("numconcurrent", "1"));
            int numThreads = Math.min(numFiles, 1000);
            byte[] data = sb.toString().getBytes(UTF_8);
            long runid = System.currentTimeMillis();
            List<Callable<Long>> clients = new ArrayList<Callable<Long>>();

            if (target.equals("bk")) {
                String zkservers = cmd.getOptionValue("zkservers", "localhost:2181");
                int bkensemble = Integer.parseInt(cmd.getOptionValue("bkensemble", "3"));
                int bkquorum = Integer.parseInt(cmd.getOptionValue("bkquorum", "2"));
                int bkthrottle = Integer.parseInt(cmd.getOptionValue("bkthrottle", "10000"));

                ClientConfiguration conf = new ClientConfiguration();
                conf.setThrottleValue(bkthrottle);
                conf.setMetadataServiceUri("zk://" + zkservers + "/ledgers");

                bkc = new BookKeeper(conf);
                List<LedgerHandle> handles = new ArrayList<LedgerHandle>();
                for (int i = 0; i < numFiles; i++) {
                    handles.add(bkc.createLedger(bkensemble, bkquorum, DigestType.CRC32, new byte[] {'a', 'b'}));
                }
                for (int i = 0; i < numFiles; i++) {
                    clients.add(new BKClient(handles, data, runfor, cmd.hasOption("sync")));
                }
            } else if (target.equals("fs")) {
                List<FileOutputStream> streams = new ArrayList<FileOutputStream>();
                for (int i = 0; i < numFiles; i++) {
                    String path = cmd.getOptionValue("path", "/foobar " + i);
                    streams.add(new FileOutputStream(path + runid + "_" + i));
                }

                for (int i = 0; i < numThreads; i++) {
                    clients.add(new FileClient(streams, data, runfor));
                }
            } else {
                LOG.error("Unknown option: " + target);
                throw new IllegalArgumentException("Unknown target " + target);
            }

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            long start = System.currentTimeMillis();

            List<Future<Long>> results = executor.invokeAll(clients,
                                                            10, TimeUnit.MINUTES);
            long end = System.currentTimeMillis();
            long count = 0;
            for (Future<Long> r : results) {
                if (!r.isDone()) {
                    LOG.warn("Job didn't complete");
                    System.exit(2);
                }
                long c = r.get();
                if (c == 0) {
                    LOG.warn("Task didn't complete");
                }
                count += c;
            }
            long time = end - start;
            LOG.info("Finished processing writes (ms): {} TPT: {} op/s", time, count / ((double) time / 1000));
            executor.shutdown();
        } catch (ExecutionException ee) {
            LOG.error("Exception in worker", ee);
        } catch (BKException e) {
            LOG.error("Error accessing bookkeeper", e);
        } catch (IOException ioe) {
            LOG.error("I/O exception during benchmark", ioe);
        } catch (InterruptedException ie) {
            LOG.error("Benchmark interrupted", ie);
            Thread.currentThread().interrupt();
        } finally {
            if (bkc != null) {
                try {
                    bkc.close();
                } catch (BKException bke) {
                    LOG.error("Error closing bookkeeper client", bke);
                } catch (InterruptedException ie) {
                    LOG.warn("Interrupted closing bookkeeper client", ie);
                    Thread.currentThread().interrupt();
                }
            }
        }
        timeouter.cancel();
    }

    static class FileClient implements Callable<Long> {
        final List<FileOutputStream> streams;
        final byte[] data;
        final long time;
        final Random r;

        FileClient(List<FileOutputStream> streams, byte[] data, long time) {
            this.streams = streams;
            this.data = data;
            this.time = time;
            this.r = new Random(System.identityHashCode(this));
        }

        @Override
        public Long call() {
            try {
                long count = 0;
                long start = System.currentTimeMillis();

                long stopat = start + time;
                while (System.currentTimeMillis() < stopat) {
                    FileOutputStream stream = streams.get(r.nextInt(streams.size()));
                    synchronized (stream) {
                        stream.write(data);
                        stream.flush();
                        stream.getChannel().force(false);
                    }
                    count++;
                }

                long time = (System.currentTimeMillis() - start);
                LOG.info("Worker finished processing writes (ms): {} TPT: {} op/s", time,
                         count / ((double) time / 1000));
                return count;
            } catch (IOException ioe) {
                LOG.error("Exception in worker thread", ioe);
                return 0L;
            }
        }
    }

    static class BKClient implements Callable<Long>, AddCallback {
        final List<LedgerHandle> handles;
        final byte[] data;
        final long time;
        final Random r;
        final boolean sync;
        final AtomicLong success = new AtomicLong(0);
        final AtomicLong outstanding = new AtomicLong(0);

        BKClient(List<LedgerHandle> handles, byte[] data, long time, boolean sync) {
            this.handles = handles;
            this.data = data;
            this.time = time;
            this.r = new Random(System.identityHashCode(this));
            this.sync = sync;
        }

        @Override
        public Long call() {
            try {
                long start = System.currentTimeMillis();

                long stopat = start + time;
                while (System.currentTimeMillis() < stopat) {
                    LedgerHandle lh = handles.get(r.nextInt(handles.size()));
                    if (sync) {
                        lh.addEntry(data);
                        success.incrementAndGet();
                    } else {
                        lh.asyncAddEntry(data, this, null);
                        outstanding.incrementAndGet();
                    }
                }

                int ticks = 10; // don't wait for more than 10 seconds
                while (outstanding.get() > 0 && ticks-- > 0) {
                    Thread.sleep(10);
                }

                long time = (System.currentTimeMillis() - start);
                LOG.info("Worker finished processing writes (ms): {} TPT: {} op/s", time,
                         success.get() / ((double) time / 1000));
                return success.get();
            } catch (BKException e) {
                LOG.error("Exception in worker thread", e);
                return 0L;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Exception in worker thread", ie);
                return 0L;
            }
        }

        @Override
        public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
            if (rc == BKException.Code.OK) {
                success.incrementAndGet();
            }
            outstanding.decrementAndGet();
        }
    }
}
