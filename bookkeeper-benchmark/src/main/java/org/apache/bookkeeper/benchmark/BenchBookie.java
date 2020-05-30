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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.SystemUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class used for benchmarking the performance of bookies.
 */
public class BenchBookie {
    static final Logger LOG = LoggerFactory.getLogger(BenchBookie.class);

    static class LatencyCallback implements WriteCallback {
        boolean complete;
        @Override
        public synchronized void writeComplete(int rc, long ledgerId, long entryId,
                BookieSocketAddress addr, Object ctx) {
            if (rc != 0) {
                LOG.error("Got error " + rc);
            }
            complete = true;
            notifyAll();
        }
        public synchronized void resetComplete() {
            complete = false;
        }
        public synchronized void waitForComplete() throws InterruptedException {
            while (!complete) {
                wait();
            }
        }
    }

    static class ThroughputCallback implements WriteCallback {
        int count;
        int waitingCount = Integer.MAX_VALUE;
        @Override
        public synchronized void writeComplete(int rc, long ledgerId, long entryId,
                BookieSocketAddress addr, Object ctx) {
            if (rc != 0) {
                LOG.error("Got error " + rc);
            }
            count++;
            if (count >= waitingCount) {
                notifyAll();
            }
        }
        public synchronized void waitFor(int count) throws InterruptedException {
            while (this.count < count) {
                waitingCount = count;
                wait(1000);
            }
            waitingCount = Integer.MAX_VALUE;
        }
    }

    private static long getValidLedgerId(String zkServers)
            throws IOException, BKException, KeeperException, InterruptedException {
        BookKeeper bkc = null;
        LedgerHandle lh = null;
        long id = 0;
        try {
            bkc = new BookKeeper(zkServers);
            lh = bkc.createLedger(1, 1, BookKeeper.DigestType.CRC32,
                                  new byte[20]);
            id = lh.getId();
            return id;
        } finally {
            if (lh != null) {
                lh.close();
            }
            if (bkc != null) {
                bkc.close();
            }
        }
    }
    /**
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args)
            throws InterruptedException, ParseException, IOException,
            BKException, KeeperException {
        Options options = new Options();
        options.addOption("host", true, "Hostname or IP of bookie to benchmark");
        options.addOption("port", true, "Port of bookie to benchmark (default 3181)");
        options.addOption("zookeeper", true, "Zookeeper ensemble, (default \"localhost:2181\")");
        options.addOption("size", true, "Size of message to send, in bytes (default 1024)");
        options.addOption("warmupCount", true, "Number of messages in warmup phase (default 999)");
        options.addOption("latencyCount", true, "Number of messages in latency phase (default 5000)");
        options.addOption("throughputCount", true, "Number of messages in throughput phase (default 50000)");
        options.addOption("help", false, "This message");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help") || !cmd.hasOption("host")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("BenchBookie <options>", options);
            System.exit(-1);
        }

        String addr = cmd.getOptionValue("host");
        int port = Integer.parseInt(cmd.getOptionValue("port", "3181"));
        int size = Integer.parseInt(cmd.getOptionValue("size", "1024"));
        String servers = cmd.getOptionValue("zookeeper", "localhost:2181");
        int warmUpCount = Integer.parseInt(cmd.getOptionValue("warmupCount", "999"));
        int latencyCount = Integer.parseInt(cmd.getOptionValue("latencyCount", "5000"));
        int throughputCount = Integer.parseInt(cmd.getOptionValue("throughputCount", "50000"));

        EventLoopGroup eventLoop;
        if (SystemUtils.IS_OS_LINUX) {
            try {
                eventLoop = new EpollEventLoopGroup();
            } catch (Throwable t) {
                LOG.warn("Could not use Netty Epoll event loop for benchmark {}", t.getMessage());
                eventLoop = new NioEventLoopGroup();
            }
        } else {
            eventLoop = new NioEventLoopGroup();
        }

        OrderedExecutor executor = OrderedExecutor.newBuilder()
                .name("BenchBookieClientScheduler")
                .numThreads(1)
                .build();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler"));

        ClientConfiguration conf = new ClientConfiguration();
        BookieClient bc = new BookieClientImpl(conf, eventLoop, PooledByteBufAllocator.DEFAULT, executor, scheduler,
                NullStatsLogger.INSTANCE);
        LatencyCallback lc = new LatencyCallback();

        ThroughputCallback tc = new ThroughputCallback();

        long ledger = getValidLedgerId(servers);
        for (long entry = 0; entry < warmUpCount; entry++) {
            ByteBuf toSend = Unpooled.buffer(size);
            toSend.resetReaderIndex();
            toSend.resetWriterIndex();
            toSend.writeLong(ledger);
            toSend.writeLong(entry);
            toSend.writerIndex(toSend.capacity());
            bc.addEntry(new BookieSocketAddress(addr, port), ledger, new byte[20],
                    entry, ByteBufList.get(toSend), tc, null, BookieProtocol.FLAG_NONE,
                    false, WriteFlag.NONE);
        }
        LOG.info("Waiting for warmup");
        tc.waitFor(warmUpCount);

        ledger = getValidLedgerId(servers);
        LOG.info("Benchmarking latency");
        long startTime = System.nanoTime();
        for (long entry = 0; entry < latencyCount; entry++) {
            ByteBuf toSend = Unpooled.buffer(size);
            toSend.resetReaderIndex();
            toSend.resetWriterIndex();
            toSend.writeLong(ledger);
            toSend.writeLong(entry);
            toSend.writerIndex(toSend.capacity());
            lc.resetComplete();
            bc.addEntry(new BookieSocketAddress(addr, port), ledger, new byte[20],
                        entry, ByteBufList.get(toSend), lc, null,
                        BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            lc.waitForComplete();
        }
        long endTime = System.nanoTime();
        LOG.info("Latency: " + (((double) (endTime - startTime)) / ((double) latencyCount)) / 1000000.0);

        ledger = getValidLedgerId(servers);
        LOG.info("Benchmarking throughput");
        startTime = System.currentTimeMillis();
        tc = new ThroughputCallback();
        for (long entry = 0; entry < throughputCount; entry++) {
            ByteBuf toSend = Unpooled.buffer(size);
            toSend.resetReaderIndex();
            toSend.resetWriterIndex();
            toSend.writeLong(ledger);
            toSend.writeLong(entry);
            toSend.writerIndex(toSend.capacity());
            bc.addEntry(new BookieSocketAddress(addr, port), ledger, new byte[20],
                    entry, ByteBufList.get(toSend), tc, null, BookieProtocol.FLAG_NONE,
                    false, WriteFlag.NONE);
        }
        tc.waitFor(throughputCount);
        endTime = System.currentTimeMillis();
        LOG.info("Throughput: " + ((long) throughputCount) * 1000 / (endTime - startTime));

        bc.close();
        scheduler.shutdown();
        eventLoop.shutdownGracefully();
        executor.shutdown();
    }

}
