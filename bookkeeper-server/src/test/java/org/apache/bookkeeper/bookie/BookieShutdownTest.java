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
import org.junit.Test;

public class BookieShutdownTest extends BookKeeperClusterTestCase {

    public BookieShutdownTest() {
        super(1);
    }

    /**
     * Test whether Bookie can be shutdown when the call comes inside bookie thread.
     * 
     * @throws Exception
     */
    @Test(timeout = 60000)
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
}
