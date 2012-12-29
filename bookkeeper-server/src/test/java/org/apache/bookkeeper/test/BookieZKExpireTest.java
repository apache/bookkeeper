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

import java.io.File;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;

import org.apache.bookkeeper.conf.ServerConfiguration;
import java.util.HashSet;
import junit.framework.TestCase;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.bookie.Bookie;

public class BookieZKExpireTest extends BookKeeperClusterTestCase {

    public BookieZKExpireTest() {
        super(0);
        // 6000 is minimum due to default tick time
        baseConf.setZkTimeout(6000);
        baseClientConf.setZkTimeout(6000);
    }

    @Test(timeout=60000)
    public void testBookieServerZKExpireBehaviour() throws Exception {
        BookieServer server = null;
        try {
            File f = File.createTempFile("bookieserver", "test");
            f.delete();
            f.mkdir();

            HashSet<Thread> threadset = new HashSet<Thread>();
            int threadCount = Thread.activeCount();
            Thread threads[] = new Thread[threadCount*2];
            threadCount = Thread.enumerate(threads);
            for(int i = 0; i < threadCount; i++) {
                if (threads[i].getName().indexOf("SendThread") != -1) {
                    threadset.add(threads[i]);
                }
            }

            ServerConfiguration conf = newServerConfiguration(PortManager.nextFreePort(),
                                                              zkUtil.getZooKeeperConnectString(), f, new File[] { f });
            server = new BookieServer(conf);
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
            threads = new Thread[threadCount*2];
            threadCount = Thread.enumerate(threads);
            for(int i = 0; i < threadCount; i++) {
                if (threads[i].getName().indexOf("SendThread") != -1
                        && !threadset.contains(threads[i])) {
                    sendthread = threads[i];
                    break;
                }
            }
            assertNotNull("Send thread not found", sendthread);

            sendthread.suspend();
            Thread.sleep(2*conf.getZkTimeout());
            sendthread.resume();

            // allow watcher thread to run
            secondsToWait = 20;
            while (server.isBookieRunning()
                   || server.isNioServerRunning()
                   || server.isRunning()) {
                Thread.sleep(1000);
                if (secondsToWait-- <= 0) {
                    break;
                }
            }
            assertFalse("Bookie should have shutdown on losing zk session", server.isBookieRunning());
            assertFalse("Nio Server should have shutdown on losing zk session", server.isNioServerRunning());
            assertFalse("Bookie Server should have shutdown on losing zk session", server.isRunning());
        } finally {
            server.shutdown();
        }
    }
}
