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
package org.apache.bookkeeper.bookie;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;

import java.io.IOException;
import org.apache.zookeeper.KeeperException;

import java.nio.ByteBuffer;
import org.junit.Test;
import org.junit.Assert;

public class BookieShutdownTest extends BookKeeperClusterTestCase {

    public BookieShutdownTest() {
        super(1);
    }

    /**
     * Test whether Bookie can be shutdown when the call comes inside bookie thread.
     * 
     * @throws Exception
     */
    @Test
    public void testBookieShutdownFromBookieThread() throws Exception {
        ServerConfiguration conf = bsConfs.get(0);
        killBookie(0);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch shutdownComplete = new CountDownLatch(1);
        Bookie bookie = new Bookie(conf) {
            @Override
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    // Ignore
                }
                triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
            }

            @Override
            synchronized int shutdown(int exitCode) {
                super.shutdown(exitCode);
                shutdownComplete.countDown();
                return exitCode;
            }
        };
        bookie.start();
        // after 1 sec stop .
        Thread.sleep(1000);
        latch.countDown();
        shutdownComplete.await(5000, TimeUnit.MILLISECONDS);
    }

    /**
     * Test whether bookieserver returns the correct error code when it crashes.
     */
    @Test(timeout=60000)
    public void testBookieServerThreadError() throws Exception {
        ServerConfiguration conf = bsConfs.get(0);
        killBookie(0);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch shutdownComplete = new CountDownLatch(1);
        // simulating ZooKeeper exception by assigning a closed zk client to bk
        BookieServer bkServer = new BookieServer(conf) {
            protected Bookie newBookie(ServerConfiguration conf)
                    throws IOException, KeeperException, InterruptedException,
                    BookieException {
                return new Bookie(conf) {
                    @Override
                    public void addEntry(ByteBuffer entry, WriteCallback cb,
                                         Object ctx, byte[] masterKey)
                            throws IOException, BookieException {
                        throw new OutOfMemoryError();
                    }
                };
            }
        };
        bkServer.start();

        LedgerHandle lh = bkc.createLedger(1, 1, BookKeeper.DigestType.CRC32, "passwd".getBytes());
        lh.asyncAddEntry("test".getBytes(), new AddCallback() {
                @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    // dont care, only trying to trigger OOM
                }
            }, null);
        bkServer.join();
        Assert.assertFalse("Should have died", bkServer.isRunning());
        Assert.assertEquals("Should have died with server exception code",
                            ExitCode.SERVER_EXCEPTION, bkServer.getExitCode());
    }
}
