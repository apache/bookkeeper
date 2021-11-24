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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.zk.ZKMetadataBookieDriver;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Testing StateManager cases.
 */
public class StateManagerTest extends BookKeeperClusterTestCase {

    @Rule
    public final TestName runtime = new TestName();
    final ServerConfiguration conf;
    MetadataBookieDriver driver;

    public StateManagerTest(){
        super(0);
        String ledgersPath = "/" + "ledgers" + runtime.getMethodName();
        baseClientConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri(ledgersPath));
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri(ledgersPath));
        conf = TestBKConfiguration.newServerConfiguration();
        driver = new ZKMetadataBookieDriver();

    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        zkUtil.createBKEnsemble("/" + runtime.getMethodName());
        File tmpDir = tmpDirs.createNew("stateManger", "test");
        conf.setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setJournalDirName(tmpDir.toString())
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (driver != null) {
            driver.close();
        }
    }

    /**
     * StateManager can transition between writable mode and readOnly mode if it was not created with readOnly mode.
     */
    @Test
    public void testNormalBookieTransitions() throws Exception {
        driver.initialize(conf, NullStatsLogger.INSTANCE);
        try (RegistrationManager rm = driver.createRegistrationManager();
             BookieStateManager stateManager = new BookieStateManager(conf, rm)) {
            rm.addRegistrationListener(() -> {
                stateManager.forceToUnregistered();
                // schedule a re-register operation
                stateManager.registerBookie(false);
            });
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
    }

    @Test
    public void testReadOnlyDisableBookieTransitions() throws Exception {
        conf.setReadOnlyModeEnabled(false);
        // readOnly disabled bk stateManager
        driver.initialize(
            conf,
            NullStatsLogger.INSTANCE);

        RegistrationManager rm = driver.createRegistrationManager();
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
        rm.addRegistrationListener(() -> {
                stateManager.forceToUnregistered();
                // schedule a re-register operation
                stateManager.registerBookie(false);
            });

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
        File tmpDir = tmpDirs.createNew("stateManger", "test-readonly");
        final ServerConfiguration readOnlyConf = TestBKConfiguration.newServerConfiguration();
        readOnlyConf.setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setJournalDirName(tmpDir.toString())
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri())
                .setForceReadOnlyBookie(true);
        driver.initialize(readOnlyConf, NullStatsLogger.INSTANCE);

        ReadOnlyBookie readOnlyBookie = TestBookieImpl.buildReadOnly(
                new TestBookieImpl.ResourceBuilder(readOnlyConf)
                .withMetadataDriver(driver).build());
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
        driver.initialize(
            conf,
            NullStatsLogger.INSTANCE);

        RegistrationManager rm = driver.createRegistrationManager();
        BookieStateManager stateManager = new BookieStateManager(conf, rm);
        rm.addRegistrationListener(() -> {
                stateManager.forceToUnregistered();
                // schedule a re-register operation
                stateManager.registerBookie(false);
            });

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
