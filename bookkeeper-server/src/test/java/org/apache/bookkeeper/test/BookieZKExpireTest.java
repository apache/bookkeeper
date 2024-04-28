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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

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
    @EnabledForJreRange(max = JRE.JAVA_17)
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
            assertNotNull(sendthread, "Send thread not found");

            log.info("Suspending threads");
            sendthread.suspend();
            Thread.sleep(12000);
            log.info("Resuming threads");
            sendthread.resume();

            // allow watcher thread to run
            Thread.sleep(3000);
            assertTrue(server.isBookieRunning(), "Bookie should not shutdown on zk timeout");
            assertTrue(server.isRunning(), "Bookie Server should not shutdown on zk timeout");
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
    @EnabledForJreRange(max = JRE.JAVA_17)
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
            assertNotNull(sendthread, "Send thread not found");

            log.info("Suspending threads");
            sendthread.suspend();
            Thread.sleep(2L * conf.getZkTimeout());
            log.info("Resuming threads");
            sendthread.resume();

            // allow watcher thread to run
            Thread.sleep(3000);
            assertFalse(server.isBookieRunning(), "Bookie should shutdown on losing zk session");
            assertFalse(server.isRunning(), "Bookie Server should shutdown on losing zk session");
        } finally {
            System.clearProperty("zookeeper.request.timeout");
            server.shutdown();
        }
    }

}
