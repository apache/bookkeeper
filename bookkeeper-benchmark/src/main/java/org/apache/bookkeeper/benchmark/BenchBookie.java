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

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchBookie {
    static Logger LOG = LoggerFactory.getLogger(BenchBookie.class);

    static class LatencyCallback implements WriteCallback {
        boolean complete;
        @Override
        synchronized public void writeComplete(int rc, long ledgerId, long entryId,
                InetSocketAddress addr, Object ctx) {
            if (rc != 0) {
                LOG.error("Got error " + rc);
            }
            complete = true;
            notifyAll();
        }
        synchronized public void resetComplete() {
            complete = false;
        }
        synchronized public void waitForComplete() throws InterruptedException {
            while(!complete) {
                wait();
            }
        }
    }

    static class ThroughputCallback implements WriteCallback {
        int count;
        int waitingCount = Integer.MAX_VALUE;
        synchronized public void writeComplete(int rc, long ledgerId, long entryId,
                InetSocketAddress addr, Object ctx) {
            if (rc != 0) {
                LOG.error("Got error " + rc);
            }
            count++;
            if (count >= waitingCount) {
                notifyAll();
            }
        }
        synchronized public void waitFor(int count) throws InterruptedException {
            while(this.count < count) {
                waitingCount = count;
                wait(1000);
            }
            waitingCount = Integer.MAX_VALUE;
        }
    }

    /**
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException, ParseException {
        Options options = new Options();
        options.addOption("host", true, "Hostname or IP of bookie to benchmark");
        options.addOption("port", true, "Port of bookie to benchmark (default 3181)");
        options.addOption("ledger", true, "Ledger Id to write to (default 1)");
        options.addOption("help", false, "This message");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help") || !cmd.hasOption("host")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("BenchBookie <options>", options);
            System.exit(-1);
        }

        String addr = cmd.getOptionValue("host");
        int port = Integer.valueOf(cmd.getOptionValue("port", "3181"));
        int ledger = Integer.valueOf(cmd.getOptionValue("ledger", "1"));

        ClientSocketChannelFactory channelFactory
            = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors
                                                .newCachedThreadPool());
        OrderedSafeExecutor executor = new OrderedSafeExecutor(1);

        ClientConfiguration conf = new ClientConfiguration();
        BookieClient bc = new BookieClient(conf, channelFactory, executor);
        LatencyCallback lc = new LatencyCallback();

        ThroughputCallback tc = new ThroughputCallback();
        int warmUpCount = 999;
        for(long entry = 0; entry < warmUpCount; entry++) {
            ChannelBuffer toSend = ChannelBuffers.buffer(128);
            toSend.resetReaderIndex();
            toSend.resetWriterIndex();
            toSend.writeLong(ledger);
            toSend.writeLong(entry);
            toSend.writerIndex(toSend.capacity());
            bc.addEntry(new InetSocketAddress(addr, port), ledger, new byte[20], 
                        entry, toSend, tc, null, BookieProtocol.FLAG_NONE);
        }
        LOG.info("Waiting for warmup");
        tc.waitFor(warmUpCount);

        LOG.info("Benchmarking latency");
        int entryCount = 5000;
        long startTime = System.nanoTime();
        for(long entry = 0; entry < entryCount; entry++) {
            ChannelBuffer toSend = ChannelBuffers.buffer(128);
            toSend.resetReaderIndex();
            toSend.resetWriterIndex();
            toSend.writeLong(ledger+1);
            toSend.writeLong(entry);
            toSend.writerIndex(toSend.capacity());
            lc.resetComplete();
            bc.addEntry(new InetSocketAddress(addr, port), ledger+1, new byte[20], 
                        entry, toSend, lc, null, BookieProtocol.FLAG_NONE);
            lc.waitForComplete();
        }
        long endTime = System.nanoTime();
        LOG.info("Latency: " + (((double)(endTime-startTime))/((double)entryCount))/1000000.0);

        entryCount = 50000;
        LOG.info("Benchmarking throughput");
        startTime = System.currentTimeMillis();
        tc = new ThroughputCallback();
        for(long entry = 0; entry < entryCount; entry++) {
            ChannelBuffer toSend = ChannelBuffers.buffer(128);
            toSend.resetReaderIndex();
            toSend.resetWriterIndex();
            toSend.writeLong(ledger+2);
            toSend.writeLong(entry);
            toSend.writerIndex(toSend.capacity());
            bc.addEntry(new InetSocketAddress(addr, port), ledger+2, new byte[20], 
                        entry, toSend, tc, null, BookieProtocol.FLAG_NONE);
        }
        tc.waitFor(entryCount);
        endTime = System.currentTimeMillis();
        LOG.info("Throughput: " + ((long)entryCount)*1000/(endTime-startTime));
    }

}
