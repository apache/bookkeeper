package org.apache.bookkeeper.proto;

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

import org.junit.*;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.conf.ServerConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the BookieServer death watcher
 */
public class TestDeathwatcher extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(TestDeathwatcher.class);

    public TestDeathwatcher() {
        super(1);
    }

    /**
     * Ensure that if the autorecovery daemon is running inside the bookie
     * then a failure/crash in the autorecovery daemon will not take down the
     * bookie also.
     */
    @Test(timeout=30000)
    public void testAutorecoveryFailureDoesntKillBookie() throws Exception {
        ServerConfiguration conf = newServerConfiguration().setAutoRecoveryDaemonEnabled(true);
        BookieServer bs = startBookie(conf);

        assertNotNull("Autorecovery daemon should exist", bs.autoRecoveryMain);
        assertTrue("Bookie should be running", bs.isBookieRunning());
        bs.autoRecoveryMain.shutdown();
        Thread.sleep(conf.getDeathWatchInterval()*2); // give deathwatcher time to run
        assertTrue("Bookie should be running", bs.isBookieRunning());
        bs.shutdown();
    }
}

