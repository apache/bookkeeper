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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

import org.junit.Test;

/**
 * Test the AuditorPeer.
 */
public class AutoRecoveryMainTest extends BookKeeperClusterTestCase {

    public AutoRecoveryMainTest() {
        super(3);
    }

    /**
     * Test the startup of the auditorElector and RW.
     */
    @Test
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
    @Test
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

}
