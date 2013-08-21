/**
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
package org.apache.bookkeeper.replication;

import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

import org.junit.Test;

/*
 * Test the AuditorPeer
 */
public class AutoRecoveryMainTest extends BookKeeperClusterTestCase {

    public AutoRecoveryMainTest() {
        super(3);
    }

    /*
     * test the startup of the auditorElector and RW.
     */
    @Test(timeout=60000)
    public void testStartup() throws Exception {
        AutoRecoveryMain main = new AutoRecoveryMain(bsConfs.get(0));
        try {
            main.start();
            Thread.sleep(500);
            assertTrue("AuditorElector should be running",
                    main.auditorElector.isRunning());
            assertTrue("Replication worker should be running",
                    main.replicationWorker.isRunning());
        } finally {
            main.shutdown();
        }
    }

    /*
     * Test the shutdown of all daemons
     */
    @Test(timeout=60000)
    public void testShutdown() throws Exception {
        AutoRecoveryMain main = new AutoRecoveryMain(bsConfs.get(0));
        main.start();
        Thread.sleep(500);
        assertTrue("AuditorElector should be running",
                main.auditorElector.isRunning());
        assertTrue("Replication worker should be running",
                main.replicationWorker.isRunning());

        main.shutdown();
        assertFalse("AuditorElector should not be running",
                main.auditorElector.isRunning());
        assertFalse("Replication worker should not be running",
                main.replicationWorker.isRunning());
    }

    /**
     * Test that, if an autorecovery looses its ZK connection/session
     * it will shutdown.
     */
    @Test(timeout=60000)
    public void testAutoRecoverySessionLoss() throws Exception {
        AutoRecoveryMain main1 = new AutoRecoveryMain(bsConfs.get(0));
        AutoRecoveryMain main2 = new AutoRecoveryMain(bsConfs.get(1));
        main1.start();
        main2.start();
        Thread.sleep(500);
        assertTrue("AuditorElectors should be running",
                main1.auditorElector.isRunning() && main2.auditorElector.isRunning());
        assertTrue("Replication workers should be running",
                main1.replicationWorker.isRunning() && main2.replicationWorker.isRunning());

        zkUtil.expireSession(main1.zk);
        zkUtil.expireSession(main2.zk);

        for (int i = 0; i < 10; i++) { // give it 10 seconds to shutdown
            if (!main1.auditorElector.isRunning()
                && !main2.auditorElector.isRunning()
                && !main1.replicationWorker.isRunning()
                && !main2.replicationWorker.isRunning()) {
                break;
            }
            Thread.sleep(1000);
        }
        assertFalse("Elector1 should have shutdown", main1.auditorElector.isRunning());
        assertFalse("Elector2 should have shutdown", main2.auditorElector.isRunning());
        assertFalse("RW1 should have shutdown", main1.replicationWorker.isRunning());
        assertFalse("RW2 should have shutdown", main2.replicationWorker.isRunning());
    }
}
