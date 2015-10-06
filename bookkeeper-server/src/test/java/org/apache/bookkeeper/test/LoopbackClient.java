package org.apache.bookkeeper.test;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests BookieClient. It just sends the a new entry to itself.
 *
 *
 *
 */

class LoopbackClient implements WriteCallback {
    private final static Logger LOG = LoggerFactory.getLogger(LoopbackClient.class);
    BookieClient client;
    static int recvTimeout = 2000;
    long begin = 0;
    int limit;
    OrderedSafeExecutor executor;

    static class Counter {
        int c;
        int limit;

        Counter(int limit) {
            this.c = 0;
            this.limit = limit;
        }

        synchronized void increment() {
            if (++c == limit)
                this.notify();
        }
    }

    LoopbackClient(ClientSocketChannelFactory channelFactory, OrderedSafeExecutor executor, long begin, int limit) throws IOException {
        this.client = new BookieClient(new ClientConfiguration(), channelFactory, executor);
        this.begin = begin;
    }

    void write(long ledgerId, long entry, byte[] data, BookieSocketAddress addr, WriteCallback cb, Object ctx)
            throws IOException, InterruptedException {
        LOG.info("Ledger id: " + ledgerId + ", Entry: " + entry);
        byte[] passwd = new byte[20];
        Arrays.fill(passwd, (byte) 'a');

        client.addEntry(addr, ledgerId, passwd, entry, ChannelBuffers.wrappedBuffer(data), cb, ctx, BookieProtocol.FLAG_NONE);
    }

    public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
        Counter counter = (Counter) ctx;
        counter.increment();
    }

    public static void main(String args[]) {
        byte[] data = new byte[Integer.parseInt(args[0])];
        Integer limit = Integer.parseInt(args[1]);
        Counter c = new Counter(limit);
        long ledgerId = Long.valueOf("0").longValue();
        long begin = System.currentTimeMillis();

        LoopbackClient lb;
        ClientSocketChannelFactory channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors
                .newCachedThreadPool());
        OrderedSafeExecutor executor = OrderedSafeExecutor.newBuilder()
                .name("BookieClientScheduler")
                .numThreads(2)
                .build();
        try {
            BookieSocketAddress addr = new BookieSocketAddress("127.0.0.1", Integer.valueOf(args[2]).intValue());
            lb = new LoopbackClient(channelFactory, executor, begin, limit.intValue());

            for (int i = 0; i < limit; i++) {
                lb.write(ledgerId, i, data, addr, lb, c);
            }

            synchronized (c) {
                c.wait();
                System.out.println("Time to write all entries: " + (System.currentTimeMillis() - begin));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
