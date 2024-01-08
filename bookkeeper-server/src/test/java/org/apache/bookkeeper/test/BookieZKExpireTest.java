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

package org.apache.bookkeeper.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.util.HashSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.MockUncleanShutdownDetection;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.common.testing.annotations.FlakyTest;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.PortManager;
import org.junit.Test;

/**
 * Test bookie expiration.
 */
@Slf4j
public class BookieZKExpireTest extends BookKeeperClusterTestCase {

    public BookieZKExpireTest() {
        super(0);
    }

    /*
    Should recover from request timeout.
    */
    @Test
    @SuppressWarnings("deprecation")
    public void testBookieServerZKRequestTimeoutBehaviour() throws Exception {
        // 6000 is minimum due to default tick time
        System.setProperty("zookeeper.request.timeout", "6000");
        baseConf.setZkTimeout(24000);
        baseClientConf.setZkTimeout(24000);
        BookieServer server = null;
        try {
            File f = tmpDirs.createNew("bookieserver", "test");

            HashSet<Thread> threadset = new HashSet<Thread>();
            int threadCount = Thread.activeCount();
            Thread[] threads = new Thread[threadCount * 2];
            threadCount = Thread.enumerate(threads);
            for (int i = 0; i < threadCount; i++) {
                if (threads[i].getName().indexOf("SendThread") != -1) {
                    threadset.add(threads[i]);
                }
            }

            ServerConfiguration conf = newServerConfiguration(PortManager.nextFreePort(), f, new File[] { f });
            server = new BookieServer(
                    conf, new TestBookieImpl(conf),
                    NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT,
                    new MockUncleanShutdownDetection());
            server.start();

            int secondsToWait = 5;
            while (!server.isRunning()) {
                Thread.sleep(1000);
                if (secondsToWait-- <= 0) {
                    fail("Bookie never started");
                }
            }
            Thread sendthread = null;
            threadCount = Thread.activeCount();
            threads = new Thread[threadCount * 2];
            threadCount = Thread.enumerate(threads);
            for (int i = 0; i < threadCount; i++) {
                if (threads[i].getName().indexOf("SendThread") != -1
                        && !threadset.contains(threads[i])) {
                    sendthread = threads[i];
                    break;
                }
            }
            assertNotNull("Send thread not found", sendthread);

            log.info("Suspending threads");
            sendthread.suspend();
            Thread.sleep(12000);
            log.info("Resuming threads");
            sendthread.resume();

            // allow watcher thread to run
            Thread.sleep(3000);
            assertTrue("Bookie should not shutdown on zk timeout", server.isBookieRunning());
            assertTrue("Bookie Server should not shutdown on zk timeout", server.isRunning());
        } finally {
            System.clearProperty("zookeeper.request.timeout");
            server.shutdown();
        }
    }

    /*
    Bookie cannot recover from ZK Client's SessionExpired error.
    In this case the ZK client must be recreated, reconnect does not work.
    Attempt to reconnect by BookieStateManager's RegistrationManager listener
    will fail (even if retry it many times).
    */
    @FlakyTest(value = "https://github.com/apache/bookkeeper/issues/4142")
    @SuppressWarnings("deprecation")
    public void testBookieServerZKSessionExpireBehaviour() throws Exception {
        // 6000 is minimum due to default tick time
        System.setProperty("zookeeper.request.timeout", "0");
        baseConf.setZkTimeout(6000);
        baseClientConf.setZkTimeout(6000);
        BookieServer server = null;
        try {
            File f = tmpDirs.createNew("bookieserver", "test");

            HashSet<Thread> threadset = new HashSet<Thread>();
            int threadCount = Thread.activeCount();
            Thread[] threads = new Thread[threadCount * 2];
            threadCount = Thread.enumerate(threads);
            for (int i = 0; i < threadCount; i++) {
                if (threads[i].getName().indexOf("SendThread") != -1) {
                    threadset.add(threads[i]);
                }
            }

            ServerConfiguration conf = newServerConfiguration(PortManager.nextFreePort(), f, new File[] { f });
            server = new BookieServer(
                    conf, new TestBookieImpl(conf),
                    NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT,
                    new MockUncleanShutdownDetection());
            server.start();

            int secondsToWait = 5;
            while (!server.isRunning()) {
                Thread.sleep(1000);
                if (secondsToWait-- <= 0) {
                    fail("Bookie never started");
                }
            }
            Thread sendthread = null;
            threadCount = Thread.activeCount();
            threads = new Thread[threadCount * 2];
            threadCount = Thread.enumerate(threads);
            for (int i = 0; i < threadCount; i++) {
                if (threads[i].getName().indexOf("SendThread") != -1
                        && !threadset.contains(threads[i])) {
                    sendthread = threads[i];
                    break;
                }
            }
            assertNotNull("Send thread not found", sendthread);

            log.info("Suspending threads");
            sendthread.suspend();
            Thread.sleep(2L * conf.getZkTimeout());
            log.info("Resuming threads");
            sendthread.resume();

            // allow watcher thread to run
            Thread.sleep(3000);
            assertFalse("Bookie should shutdown on losing zk session", server.isBookieRunning());
            assertFalse("Bookie Server should shutdown on losing zk session", server.isRunning());
        } finally {
            System.clearProperty("zookeeper.request.timeout");
            server.shutdown();
        }
    }

}
