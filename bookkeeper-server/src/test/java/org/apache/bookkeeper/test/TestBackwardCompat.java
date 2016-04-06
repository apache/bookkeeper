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
package org.apache.bookkeeper.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.File;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.FileSystemUpgrade;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBackwardCompat {
    private final static Logger LOG = LoggerFactory.getLogger(TestBackwardCompat.class);

    private static ZooKeeperUtil zkUtil = new ZooKeeperUtil();;
    private static byte[] ENTRY_DATA = "ThisIsAnEntry".getBytes();

    static void waitUp(int port) throws Exception {
        while(zkUtil.getZooKeeperClient().exists(
                      "/ledgers/available/" + InetAddress.getLocalHost().getHostAddress() + ":" + port,
                      false) == null) {
            Thread.sleep(500);
        }
    }

    final List<File> tempDirs = new ArrayList<File>();

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    @Before
    public void startZooKeeperServer() throws Exception {
        zkUtil.startServer();
    }

    @After
    public void stopZooKeeperServer() throws Exception {
        zkUtil.killServer();
        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }

    /**
     * Version 4.0.0 classes
     */
    static class Server400 {
        org.apache.bk_v4_0_0.bookkeeper.conf.ServerConfiguration conf;
        org.apache.bk_v4_0_0.bookkeeper.proto.BookieServer server = null;

        Server400(File journalDir, File ledgerDir, int port) throws Exception {
            conf = new org.apache.bk_v4_0_0.bookkeeper.conf.ServerConfiguration();
            conf.setBookiePort(port);
            conf.setZkServers(zkUtil.getZooKeeperConnectString());
            conf.setJournalDirName(journalDir.getPath());
            conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });
        }

        void start() throws Exception {
            server = new org.apache.bk_v4_0_0.bookkeeper.proto.BookieServer(conf);
            server.start();
            waitUp(conf.getBookiePort());
        }

        org.apache.bk_v4_0_0.bookkeeper.conf.ServerConfiguration getConf() {
            return conf;
        }

        void stop() throws Exception {
            if (server != null) {
                server.shutdown();
            }
        }
    }

    static class Ledger400 {
        org.apache.bk_v4_0_0.bookkeeper.client.BookKeeper bk;
        org.apache.bk_v4_0_0.bookkeeper.client.LedgerHandle lh;

        private Ledger400(org.apache.bk_v4_0_0.bookkeeper.client.BookKeeper bk,
                          org.apache.bk_v4_0_0.bookkeeper.client.LedgerHandle lh) {
            this.bk = bk;
            this.lh = lh;
        }

        static Ledger400 newLedger() throws Exception {
            org.apache.bk_v4_0_0.bookkeeper.client.BookKeeper newbk
                = new org.apache.bk_v4_0_0.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bk_v4_0_0.bookkeeper.client.LedgerHandle newlh
                = newbk.createLedger(1, 1,
                                  org.apache.bk_v4_0_0.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                  "foobar".getBytes());
            return new Ledger400(newbk, newlh);
        }

        static Ledger400 openLedger(long id) throws Exception {
            org.apache.bk_v4_0_0.bookkeeper.client.BookKeeper newbk
                = new org.apache.bk_v4_0_0.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bk_v4_0_0.bookkeeper.client.LedgerHandle newlh
                = newbk.openLedger(id,
                                org.apache.bk_v4_0_0.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                "foobar".getBytes());
            return new Ledger400(newbk, newlh);
        }

        long getId() {
            return lh.getId();
        }

        void write100() throws Exception {
            for (int i = 0; i < 100; i++) {
                lh.addEntry(ENTRY_DATA);
            }
        }

        long readAll() throws Exception {
            long count = 0;
            Enumeration<org.apache.bk_v4_0_0.bookkeeper.client.LedgerEntry> entries
                = lh.readEntries(0, lh.getLastAddConfirmed());
            while (entries.hasMoreElements()) {
                assertTrue("entry data doesn't match",
                           Arrays.equals(entries.nextElement().getEntry(), ENTRY_DATA));
                count++;
            }
            return count;
        }

        void close() throws Exception {
            try {
                if (lh != null) {
                    lh.close();
                }
            } finally {
                if (bk != null) {
                    bk.close();
                }
            }
        }
    }

    /**
     * Version 4.1.0 classes
     */
    static class Server410 {
        org.apache.bk_v4_1_0.bookkeeper.conf.ServerConfiguration conf;
        org.apache.bk_v4_1_0.bookkeeper.proto.BookieServer server = null;

        Server410(File journalDir, File ledgerDir, int port) throws Exception {
            conf = new org.apache.bk_v4_1_0.bookkeeper.conf.ServerConfiguration();
            conf.setBookiePort(port);
            conf.setZkServers(zkUtil.getZooKeeperConnectString());
            conf.setJournalDirName(journalDir.getPath());
            conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });
        }

        void start() throws Exception {
            server = new org.apache.bk_v4_1_0.bookkeeper.proto.BookieServer(conf);
            server.start();
            waitUp(conf.getBookiePort());
        }

        org.apache.bk_v4_1_0.bookkeeper.conf.ServerConfiguration getConf() {
            return conf;
        }

        void stop() throws Exception {
            if (server != null) {
                server.shutdown();
            }
        }
    }

    static class Ledger410 {
        org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper bk;
        org.apache.bk_v4_1_0.bookkeeper.client.LedgerHandle lh;

        private Ledger410(org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper bk,
                          org.apache.bk_v4_1_0.bookkeeper.client.LedgerHandle lh) {
            this.bk = bk;
            this.lh = lh;
        }

        static Ledger410 newLedger() throws Exception {
            org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper newbk
                = new org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bk_v4_1_0.bookkeeper.client.LedgerHandle newlh
                = newbk.createLedger(1, 1,
                                  org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                  "foobar".getBytes());
            return new Ledger410(newbk, newlh);
        }

        static Ledger410 openLedger(long id) throws Exception {
            org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper newbk
                = new org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bk_v4_1_0.bookkeeper.client.LedgerHandle newlh
                = newbk.openLedger(id,
                                org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                "foobar".getBytes());
            return new Ledger410(newbk, newlh);
        }

        long getId() {
            return lh.getId();
        }

        void write100() throws Exception {
            for (int i = 0; i < 100; i++) {
                lh.addEntry(ENTRY_DATA);
            }
        }

        long readAll() throws Exception {
            long count = 0;
            Enumeration<org.apache.bk_v4_1_0.bookkeeper.client.LedgerEntry> entries
                = lh.readEntries(0, lh.getLastAddConfirmed());
            while (entries.hasMoreElements()) {
                assertTrue("entry data doesn't match",
                           Arrays.equals(entries.nextElement().getEntry(), ENTRY_DATA));
                count++;
            }
            return count;
        }

        void close() throws Exception {
            try {
                if (lh != null) {
                    lh.close();
                }
            } finally {
                if (bk != null) {
                    bk.close();
                }
            }
        }
    }

    /**
     * Version 4.2.0 classes
     */
    static class Server420 {
        org.apache.bk_v4_2_0.bookkeeper.conf.ServerConfiguration conf;
        org.apache.bk_v4_2_0.bookkeeper.proto.BookieServer server = null;

        Server420(File journalDir, File ledgerDir, int port) throws Exception {
            conf = new org.apache.bk_v4_2_0.bookkeeper.conf.ServerConfiguration();
            conf.setBookiePort(port);
            conf.setZkServers(zkUtil.getZooKeeperConnectString());
            conf.setJournalDirName(journalDir.getPath());
            conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });
        }

        void start() throws Exception {
            server = new org.apache.bk_v4_2_0.bookkeeper.proto.BookieServer(conf);
            server.start();
            waitUp(conf.getBookiePort());
        }

        org.apache.bk_v4_2_0.bookkeeper.conf.ServerConfiguration getConf() {
            return conf;
        }

        void stop() throws Exception {
            if (server != null) {
                server.shutdown();
            }
        }
    }

    static class Ledger420 {
        org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper bk;
        org.apache.bk_v4_2_0.bookkeeper.client.LedgerHandle lh;

        private Ledger420(org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper bk,
                          org.apache.bk_v4_2_0.bookkeeper.client.LedgerHandle lh) {
            this.bk = bk;
            this.lh = lh;
        }

        static Ledger420 newLedger() throws Exception {
            org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper newbk
                = new org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bk_v4_2_0.bookkeeper.client.LedgerHandle newlh
                = newbk.createLedger(1, 1,
                                  org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                  "foobar".getBytes());
            return new Ledger420(newbk, newlh);
        }

        static Ledger420 openLedger(long id) throws Exception {
            org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper newbk
                = new org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bk_v4_2_0.bookkeeper.client.LedgerHandle newlh
                = newbk.openLedger(id,
                                org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                "foobar".getBytes());
            return new Ledger420(newbk, newlh);
        }

        long getId() {
            return lh.getId();
        }

        void write100() throws Exception {
            for (int i = 0; i < 100; i++) {
                lh.addEntry(ENTRY_DATA);
            }
        }

        long readAll() throws Exception {
            long count = 0;
            Enumeration<org.apache.bk_v4_2_0.bookkeeper.client.LedgerEntry> entries
                = lh.readEntries(0, lh.getLastAddConfirmed());
            while (entries.hasMoreElements()) {
                assertTrue("entry data doesn't match",
                           Arrays.equals(entries.nextElement().getEntry(), ENTRY_DATA));
                count++;
            }
            return count;
        }

        void close() throws Exception {
            try {
                if (lh != null) {
                    lh.close();
                }
            } finally {
                if (bk != null) {
                    bk.close();
                }
            }
        }
    }

    /**
     * Current verion classes
     */
    static class ServerCurrent {
        org.apache.bookkeeper.conf.ServerConfiguration conf;
        org.apache.bookkeeper.proto.BookieServer server = null;

        ServerCurrent(File journalDir, File ledgerDir, int port,
                boolean useHostNameAsBookieID) throws Exception {
            conf = TestBKConfiguration.newServerConfiguration();
            conf.setBookiePort(port);
            conf.setZkServers(zkUtil.getZooKeeperConnectString());
            conf.setJournalDirName(journalDir.getPath());
            conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });
            conf.setUseHostNameAsBookieID(useHostNameAsBookieID);
        }

        void start() throws Exception {
            server = new org.apache.bookkeeper.proto.BookieServer(conf);
            server.start();
            waitUp(conf.getBookiePort());
        }

        org.apache.bookkeeper.conf.ServerConfiguration getConf() {
            return conf;
        }

        void stop() throws Exception {
            if (server != null) {
                server.shutdown();
            }
        }
    }

    static class LedgerCurrent {
        org.apache.bookkeeper.client.BookKeeper bk;
        org.apache.bookkeeper.client.LedgerHandle lh;

        private LedgerCurrent(org.apache.bookkeeper.client.BookKeeper bk,
                              org.apache.bookkeeper.client.LedgerHandle lh) {
            this.bk = bk;
            this.lh = lh;
        }

        static LedgerCurrent newLedger() throws Exception {
            org.apache.bookkeeper.client.BookKeeper newbk
                = new org.apache.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bookkeeper.client.LedgerHandle newlh
                = newbk.createLedger(1, 1,
                                     org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                     "foobar".getBytes());
            return new LedgerCurrent(newbk, newlh);
        }

        static LedgerCurrent openLedger(long id) throws Exception {
            org.apache.bookkeeper.client.BookKeeper newbk
                = new org.apache.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bookkeeper.client.LedgerHandle newlh
                = newbk.openLedger(id,
                                org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                "foobar".getBytes());
            return new LedgerCurrent(newbk, newlh);
        }

        long getId() {
            return lh.getId();
        }

        void write100() throws Exception {
            for (int i = 0; i < 100; i++) {
                lh.addEntry(ENTRY_DATA);
            }
        }

        long readAll() throws Exception {
            long count = 0;
            Enumeration<org.apache.bookkeeper.client.LedgerEntry> entries
                = lh.readEntries(0, lh.getLastAddConfirmed());
            while (entries.hasMoreElements()) {
                assertTrue("entry data doesn't match",
                           Arrays.equals(entries.nextElement().getEntry(), ENTRY_DATA));
                count++;
            }
            return count;
        }

        void close() throws Exception {
            try {
                if (lh != null) {
                    lh.close();
                }
            } finally {
                if (bk != null) {
                    bk.close();
                }
            }
        }
    }

    /*
     * Test old cookie accessing the new version formatted cluster.
     */
    @Test(timeout=60000)
    public void testOldCookieAccessingNewCluster() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        File ledgerDir = createTempDir("bookie", "ledger");

        int port = PortManager.nextFreePort();

        // start old server
        Server410 s410 = new Server410(journalDir, ledgerDir, port);
        s410.start();

        Ledger410 l410 = Ledger410.newLedger();
        l410.write100();
        l410.getId();
        l410.close();
        s410.stop();

        // Format the metadata using current version
        ServerCurrent currentServer = new ServerCurrent(journalDir, ledgerDir,
                port, false);
        BookKeeperAdmin.format(new ClientConfiguration(currentServer.conf),
                false, true);
        // start the current version server with old version cookie
        try {
            currentServer.start();
            fail("Bookie should not start with old cookie");
        } catch (BookieException e) {
            assertTrue("Old Cookie should not be able to access", e
                    .getMessage().contains("instanceId"));
        } finally {
            currentServer.stop();
        }

        // Format the bookie also and restart
        assertTrue("Format should be success",
                Bookie.format(currentServer.conf, false, true));
        try {
            currentServer = null;
            currentServer = new ServerCurrent(journalDir, ledgerDir, port, false);
            currentServer.start();
        } finally {
            if (null != currentServer) {
                currentServer.stop();
            }
        }
    }

    /**
     * Test compatability between version 4.0.0 and the current version.
     * Incompatabilities are:
     *  - Current client will not be able to talk to 4.0.0 server.
     *  - 4.0.0 client will not be able to fence ledgers on current server.
     *  - Current server won't start with 4.0.0 server directories without upgrade.
     */
    @Test(timeout=60000)
    public void testCompat400() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        File ledgerDir = createTempDir("bookie", "ledger");

        int port = PortManager.nextFreePort();
        // start server, upgrade
        Server400 s400 = new Server400(journalDir, ledgerDir, port);
        s400.start();

        Ledger400 l400 = Ledger400.newLedger();
        l400.write100();
        long oldLedgerId = l400.getId();
        l400.close();

        // Check that current client isn't able to write to old server
        LedgerCurrent lcur = LedgerCurrent.newLedger();
        try {
            lcur.write100();
            fail("Current shouldn't be able to write to 4.0.0 server");
        } catch (Exception e) {
        }
        lcur.close();

        s400.stop();

        // Start the current server, will require a filesystem upgrade
        ServerCurrent scur = new ServerCurrent(journalDir, ledgerDir, port, false);
        try {
            scur.start();
            fail("Shouldn't be able to start without directory upgrade");
        } catch (Exception e) {
        }
        FileSystemUpgrade.upgrade(scur.getConf());

        scur.start();

        // check that old client can read its old ledgers on new server
        l400 = Ledger400.openLedger(oldLedgerId);
        assertEquals(100, l400.readAll());
        l400.close();

        // check that old client can create ledgers on new server
        l400 = Ledger400.newLedger();
        l400.write100();
        l400.close();

        // check that current client can read old ledger
        lcur = LedgerCurrent.openLedger(oldLedgerId);
        assertEquals(100, lcur.readAll());
        lcur.close();

        // check that old client can read current client's ledgers
        lcur = LedgerCurrent.openLedger(oldLedgerId);
        assertEquals(100, lcur.readAll());
        lcur.close();

        // check that old client can not fence a current client
        // due to lack of password
        lcur = LedgerCurrent.newLedger();
        lcur.write100();
        long fenceLedgerId = lcur.getId();
        try {
            l400 = Ledger400.openLedger(fenceLedgerId);
            fail("Shouldn't be able to open ledger");
        } catch (Exception e) {
            // correct behaviour
        }
        lcur.write100();
        lcur.close();

        lcur = LedgerCurrent.openLedger(fenceLedgerId);
        assertEquals(200, lcur.readAll());
        lcur.close();

        scur.stop();
    }

    /**
     * Test compatability between version 4.1.0 and the current version.
     *  - A 4.1.0 client is not able to open a ledger created by the current
     *    version due to a change in the ledger metadata format.
     *  - Otherwise, they should be compatible.
     */
    @Test(timeout=60000)
    public void testCompat410() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        File ledgerDir = createTempDir("bookie", "ledger");

        int port = PortManager.nextFreePort();
        // start server, upgrade
        Server410 s410 = new Server410(journalDir, ledgerDir, port);
        s410.start();

        Ledger410 l410 = Ledger410.newLedger();
        l410.write100();
        long oldLedgerId = l410.getId();
        l410.close();

        // Check that current client can to write to old server
        LedgerCurrent lcur = LedgerCurrent.newLedger();
        try {
            lcur.write100();
            fail("Shouldn't be able to write");
        } catch (Exception e) {
            // correct behaviour
        }
        lcur.close();

        s410.stop();

        // Start the current server, will not require a filesystem upgrade
        ServerCurrent scur = new ServerCurrent(journalDir, ledgerDir, port, false);
        scur.start();

        // check that old client can read its old ledgers on new server
        l410 = Ledger410.openLedger(oldLedgerId);
        assertEquals(100, l410.readAll());
        l410.close();

        // check that old client can create ledgers on new server
        l410 = Ledger410.newLedger();
        l410.write100();
        l410.close();

        // check that an old client can fence an old client
        l410 = Ledger410.newLedger();
        l410.write100();

        Ledger410 l410f = Ledger410.openLedger(l410.getId());
        try {
            l410.write100();
            fail("Shouldn't be able to write");
        } catch (Exception e) {
            // correct behaviour
        }
        l410f.close();
        try {
            l410.close();
            fail("Shouldn't be able to close");
        } catch (Exception e) {
            // correct
        }

        // check that a new client can fence an old client
        // and the old client can continue to read that ledger
        l410 = Ledger410.newLedger();
        l410.write100();

        oldLedgerId = l410.getId();
        lcur = LedgerCurrent.openLedger(oldLedgerId);
        try {
            l410.write100();
            fail("Shouldn't be able to write");
        } catch (Exception e) {
            // correct behaviour
        }
        try {
            l410.close();
            fail("Shouldn't be able to close");
        } catch (Exception e) {
            // correct
        }
        lcur.close();

        l410 = Ledger410.openLedger(oldLedgerId);

        assertEquals(100, l410.readAll());
        l410.close();

        // check that current client can read old ledger
        lcur = LedgerCurrent.openLedger(oldLedgerId);
        assertEquals(100, lcur.readAll());
        lcur.close();

        // check that old client can read current client's ledgers
        lcur = LedgerCurrent.openLedger(oldLedgerId);
        assertEquals(100, lcur.readAll());
        lcur.close();

        // check that old client can not fence a current client
        // since it cannot open a new ledger due to the format changes
        lcur = LedgerCurrent.newLedger();
        lcur.write100();
        long fenceLedgerId = lcur.getId();
        try {
            l410 = Ledger410.openLedger(fenceLedgerId);
            fail("Shouldn't be able to open ledger");
        } catch (Exception e) {
            // correct behaviour
        }
        lcur.write100();
        lcur.close();

        scur.stop();
    }

    /**
     * Test compatability between old versions and the current version.
     * - old server restarts with useHostNameAsBookieID=true.
     * - Read ledgers with old and new clients
     */
    @Test(timeout = 60000)
    public void testCompatReads() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        File ledgerDir = createTempDir("bookie", "ledger");

        int port = PortManager.nextFreePort();
        // start server, upgrade
        Server410 s410 = new Server410(journalDir, ledgerDir, port);
        s410.start();

        Ledger410 l410 = Ledger410.newLedger();
        l410.write100();
        long oldLedgerId = l410.getId();
        l410.close();

        // Check that 420 client can to write to 410 server
        Ledger420 l420 = Ledger420.newLedger();
        l420.write100();
        long lid420 = l420.getId();
        l420.close();

        s410.stop();

        // Start the current server, will not require a filesystem upgrade
        ServerCurrent scur = new ServerCurrent(journalDir, ledgerDir, port,
                true);
        scur.start();

        // check that old client can read its old ledgers on new server
        l410 = Ledger410.openLedger(oldLedgerId);
        assertEquals(100, l410.readAll());
        l410.close();

        // Check that 420 client can read old ledgers on new server
        l420 = Ledger420.openLedger(lid420);
        assertEquals("Failed to read entries!", 100, l420.readAll());
        l420.close();

        // Check that current client can read old ledgers on new server
        final LedgerCurrent curledger = LedgerCurrent.openLedger(lid420);
        assertEquals("Failed to read entries!", 100, curledger.readAll());
        curledger.close();
    }

    /**
     * Test compatability between version old version and the current version.
     * - 4.1.0 server restarts with useHostNameAsBookieID=true.
     * - Write ledgers with old and new clients
     * - Read ledgers written by old clients.
     */
    @Test(timeout = 60000)
    public void testCompatWrites() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        File ledgerDir = createTempDir("bookie", "ledger");

        int port = PortManager.nextFreePort();
        // start server, upgrade
        Server410 s410 = new Server410(journalDir, ledgerDir, port);
        s410.start();
        s410.stop();

        // Start the current server, will not require a filesystem upgrade
        ServerCurrent scur = new ServerCurrent(journalDir, ledgerDir, port,
                true);
        scur.start();

        // Check that current client can to write to server
        LedgerCurrent lcur = LedgerCurrent.newLedger();
        lcur.write100();
        lcur.close();
        final LedgerCurrent curledger = LedgerCurrent.openLedger(lcur.getId());
        assertEquals("Failed to read entries!", 100, curledger.readAll());

        // Check that 410 client can write to server
        Ledger410 l410 = Ledger410.newLedger();
        l410.write100();
        long oldLedgerId = l410.getId();
        l410.close();

        // Check that 420 client can write to server
        Ledger410 l420 = Ledger410.newLedger();
        l420.write100();
        long lid420 = l420.getId();
        l420.close();

        // check that new client can read old ledgers on new server
        LedgerCurrent oldledger = LedgerCurrent.openLedger(oldLedgerId);
        assertEquals("Failed to read entries!", 100, oldledger.readAll());
        oldledger.close();

        // check that new client can read old ledgers on new server
        oldledger = LedgerCurrent.openLedger(lid420);
        assertEquals("Failed to read entries!", 100, oldledger.readAll());
        oldledger.close();
        scur.stop();
    }
}
