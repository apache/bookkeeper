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
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing StateManager cases.
 */
public class StateManagerTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(StateManagerTest.class);

    @Rule
    public final TestName runtime = new TestName();
    MockZKRegistrationManager rm;

    public StateManagerTest() {
        super(0);
        String ledgersPath = "/" + "ledgers" + runtime.getMethodName();
        baseClientConf.setZkLedgersRootPath(ledgersPath);
        baseConf.setZkLedgersRootPath(ledgersPath);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        zkUtil.createBKEnsemble("/" + runtime.getMethodName());
        rm = new MockZKRegistrationManager();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (rm != null) {
            rm.close();
        }
    }

    private static class MockZKRegistrationManager extends ZKRegistrationManager {
        boolean registerFailed = false;
        void setRegisterFail(boolean failOrNot){
            registerFailed = failOrNot;
        }
        @Override
        public void registerBookie(String bookieId, boolean readOnly) throws BookieException {
            if (registerFailed) {
                throw BookieException.create(-100);
            }
            super.registerBookie(bookieId, readOnly);
        }

    }

    /**
     * Bookie should shutdown when it register to Registration service fail.
     * On ZooKeeper exception, should return exit code ZK_REG_FAIL = 4
     */
    @Test
    public void testShutdown() throws Exception {
        File tmpDir = createTempDir("stateManger", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() })
            .setZkServers(zkUtil.getZooKeeperConnectString());

        BookieServer bkServer = new BookieServer(conf) {
            protected Bookie newBookie(ServerConfiguration conf)
                    throws IOException, KeeperException, InterruptedException,
                    BookieException {
                Bookie bookie = new Bookie(conf);
                rm.setRegisterFail(true);
                rm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
                LOG.info(" is {} ", zkc == null);
                bookie.setRegistrationManager(rm);
                return bookie;
            }
        };
        bkServer.start();
        bkServer.join();
        assertTrue("Failed to return failCode ZK_REG_FAIL",
                ExitCode.ZK_REG_FAIL == bkServer.getExitCode());
    }

    /**
     * StateManager can transition between writable mode and readOnly mode if it was not created with readOnly mode.
     */
    @Test
    public void testBookieTransitions() throws Exception {
        File tmpDir = createTempDir("stateManger", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setZkServers(zkUtil.getZooKeeperConnectString());
        Bookie bookie = new Bookie(conf);
        bookie.start();
        assertTrue(bookie.isRunning());

        bookie.getStateManager().transitionToReadOnlyMode();
        //sleep a little to wait transition finish
        Thread.sleep(1000);
        assertTrue(bookie.isRunning());
        assertTrue(bookie.isReadOnly());

        bookie.getStateManager().transitionToWritableMode();
        Thread.sleep(1000);
        assertTrue(bookie.isRunning());
        assertFalse(bookie.isReadOnly());

        bookie.shutdown();
        assertFalse(bookie.isRunning());

        // readOnly disabled bk
        conf.setReadOnlyModeEnabled(false);
        Bookie bookie2 = new Bookie(conf);
        bookie2.start();
        assertTrue(bookie2.isRunning());

        bookie2.getStateManager().transitionToReadOnlyMode();
        //sleep a little to wait transition finish
        Thread.sleep(1000);
        // bookie2 will shutdown
        assertFalse(bookie2.isRunning());
        // different dimension of bookie state: running <--> down, read <--> write, unregistered <--> registered
        // bookie2 is set to readOnly when shutdown
        assertTrue(bookie2.isReadOnly());

        // readOnlybk
        final ServerConfiguration readOnlyConf = TestBKConfiguration.newServerConfiguration();
        readOnlyConf.setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setForceReadOnlyBookie(true);
        ReadOnlyBookie readOnlyBookie = new ReadOnlyBookie(readOnlyConf, NullStatsLogger.INSTANCE);
        readOnlyBookie.start();
        assertTrue(readOnlyBookie.isRunning());
        assertTrue(readOnlyBookie.isReadOnly());

        // transition has no effect if bookie start with readOnly mode
        readOnlyBookie.getStateManager().transitionToWritableMode();
        Thread.sleep(1000);
        assertTrue(readOnlyBookie.isRunning());
        assertTrue(readOnlyBookie.isReadOnly());
        readOnlyBookie.shutdown();

    }

    /**
     * Verify the bookie reg.
     */
    @Test
    public void testRegistration() throws Exception {
        File tmpDir = createTempDir("stateManger", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setZkServers(zkUtil.getZooKeeperConnectString());
        Bookie bookie = new Bookie(conf);
        // -1: unregistered
        assertEquals(-1, bookie.getStateManager().getState());
        bookie.start();
        assertTrue(bookie.isRunning());
        // 1: up
        assertEquals(1, bookie.getStateManager().getState());
        bookie.shutdown();
        // 0: readOnly
        assertEquals(0, bookie.getStateManager().getState());
    }

}
