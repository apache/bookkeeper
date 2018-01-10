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

import static org.apache.bookkeeper.bookie.BookieException.Code.MetadataStoreException;
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
    final ServerConfiguration conf;
    MockZKRegistrationManager rm;

    public StateManagerTest(){
        super(0);
        String ledgersPath = "/" + "ledgers" + runtime.getMethodName();
        baseClientConf.setZkLedgersRootPath(ledgersPath);
        baseConf.setZkLedgersRootPath(ledgersPath);
        conf = TestBKConfiguration.newServerConfiguration();

    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        zkUtil.createBKEnsemble("/" + runtime.getMethodName());
        rm = new MockZKRegistrationManager();
        File tmpDir = createTempDir("stateManger", "test");
        conf.setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setJournalDirName(tmpDir.toString())
                .setZkServers(zkUtil.getZooKeeperConnectString());
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
                throw BookieException.create(MetadataStoreException);
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
            .setJournalDirName(tmpDir.toString())
            .setZkServers(zkUtil.getZooKeeperConnectString());

        BookieServer bkServer = new BookieServer(conf) {
            protected Bookie newBookie(ServerConfiguration conf)
                    throws IOException, KeeperException, InterruptedException,
                    BookieException {
                Bookie bookie = new Bookie(conf);
                rm.setRegisterFail(true);
                rm.initialize(conf, () -> {}, NullStatsLogger.INSTANCE);
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
    public void testNormalBookieTransitions() throws Exception {
        BookieStateManager stateManager = new BookieStateManager(conf, rm);
        rm.initialize(conf, () -> {
            stateManager.forceToUnregistered();
            // schedule a re-register operation
            stateManager.registerBookie(false);
        }, NullStatsLogger.INSTANCE);

        stateManager.initState();
        stateManager.registerBookie(true).get();

        assertTrue(stateManager.isRunning());
        assertTrue(stateManager.isRegistered());

        stateManager.transitionToReadOnlyMode().get();
        assertTrue(stateManager.isReadOnly());

        stateManager.transitionToWritableMode().get();
        assertTrue(stateManager.isRunning());
        assertFalse(stateManager.isReadOnly());

        stateManager.close();
        assertFalse(stateManager.isRunning());
    }

    @Test
    public void testReadOnlyDisableBookieTransitions() throws Exception {
        conf.setReadOnlyModeEnabled(false);
        // readOnly disabled bk stateManager
        BookieStateManager stateManager = new BookieStateManager(conf, rm);
        // simulate sync shutdown logic in bookie
        stateManager.setShutdownHandler(new StateManager.ShutdownHandler() {
            @Override
            public void shutdown(int code) {
                try {
                    if (stateManager.isRunning()) {
                        stateManager.forceToShuttingDown();
                        stateManager.forceToReadOnly();
                    }

                } finally {
                    stateManager.close();
                }
            }
        });
        rm.initialize(conf, () -> {
            stateManager.forceToUnregistered();
            // schedule a re-register operation
            stateManager.registerBookie(false);
        }, NullStatsLogger.INSTANCE);

        stateManager.initState();
        stateManager.registerBookie(true).get();
        assertTrue(stateManager.isRunning());

        stateManager.transitionToReadOnlyMode().get();
        // stateManager2 will shutdown
        assertFalse(stateManager.isRunning());
        // different dimension of bookie state: running <--> down, read <--> write, unregistered <--> registered
        // bookie2 is set to readOnly when shutdown
        assertTrue(stateManager.isReadOnly());
    }

    @Test
    public void testReadOnlyBookieTransitions() throws Exception{
        // readOnlybk, which use override stateManager impl
        File tmpDir = createTempDir("stateManger", "test-readonly");
        final ServerConfiguration readOnlyConf = TestBKConfiguration.newServerConfiguration();
        readOnlyConf.setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setJournalDirName(tmpDir.toString())
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setForceReadOnlyBookie(true);
        ReadOnlyBookie readOnlyBookie = new ReadOnlyBookie(readOnlyConf, NullStatsLogger.INSTANCE);
        readOnlyBookie.start();
        assertTrue(readOnlyBookie.isRunning());
        assertTrue(readOnlyBookie.isReadOnly());

        // transition has no effect if bookie start with readOnly mode
        readOnlyBookie.getStateManager().transitionToWritableMode().get();
        assertTrue(readOnlyBookie.isRunning());
        assertTrue(readOnlyBookie.isReadOnly());
        readOnlyBookie.shutdown();

    }

    /**
     * Verify the bookie reg.
     */
    @Test
    public void testRegistration() throws Exception {
        BookieStateManager stateManager = new BookieStateManager(conf, rm);
        rm.initialize(conf, () -> {
            stateManager.forceToUnregistered();
            // schedule a re-register operation
            stateManager.registerBookie(false);
        }, NullStatsLogger.INSTANCE);
        // simulate sync shutdown logic in bookie
        stateManager.setShutdownHandler(new StateManager.ShutdownHandler() {
            @Override
            public void shutdown(int code) {
                try {
                    if (stateManager.isRunning()) {
                        stateManager.forceToShuttingDown();
                        stateManager.forceToReadOnly();
                    }

                } finally {
                    stateManager.close();
                }
            }
        });
        stateManager.initState();
        // up
        assertTrue(stateManager.isRunning());
        // unregistered
        assertFalse(stateManager.isRegistered());

        stateManager.registerBookie(true).get();
        // registered
        assertTrue(stateManager.isRegistered());
        stateManager.getShutdownHandler().shutdown(ExitCode.OK);
        // readOnly
        assertTrue(stateManager.isReadOnly());
    }

}
