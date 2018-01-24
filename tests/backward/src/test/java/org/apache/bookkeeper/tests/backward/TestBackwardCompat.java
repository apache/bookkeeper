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
package org.apache.bookkeeper.tests.backward;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test backward compat between versions.
 */
public class TestBackwardCompat {

    private static final ZooKeeperUtil zkUtil = new ZooKeeperUtil();
    private static final byte[] ENTRYDATA = "ThisIsAnEntry".getBytes();

    static void waitUp(int port) throws Exception {
        while (zkUtil.getZooKeeperClient().exists(
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
     * Version 4.1.0 classes.
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
            org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper newbk =
                    new org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bk_v4_1_0.bookkeeper.client.LedgerHandle newlh =
                    newbk.createLedger(1, 1,
                                  org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                  "foobar".getBytes());
            return new Ledger410(newbk, newlh);
        }

        static Ledger410 openLedger(long id) throws Exception {
            org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper newbk =
                    new org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bk_v4_1_0.bookkeeper.client.LedgerHandle newlh =
                    newbk.openLedger(id,
                                org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                "foobar".getBytes());
            return new Ledger410(newbk, newlh);
        }

        long getId() {
            return lh.getId();
        }

        void write100() throws Exception {
            for (int i = 0; i < 100; i++) {
                lh.addEntry(ENTRYDATA);
            }
        }

        long readAll() throws Exception {
            long count = 0;
            Enumeration<org.apache.bk_v4_1_0.bookkeeper.client.LedgerEntry> entries =
                    lh.readEntries(0, lh.getLastAddConfirmed());
            while (entries.hasMoreElements()) {
                assertTrue("entry data doesn't match",
                           Arrays.equals(entries.nextElement().getEntry(), ENTRYDATA));
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
     * Version 4.2.0 classes.
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
            conf.setDiskUsageThreshold(0.999f);
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
            org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper newbk =
                    new org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bk_v4_2_0.bookkeeper.client.LedgerHandle newlh =
                    newbk.createLedger(1, 1,
                                  org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                  "foobar".getBytes());
            return new Ledger420(newbk, newlh);
        }

        static Ledger420 openLedger(long id) throws Exception {
            org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper newbk =
                    new org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bk_v4_2_0.bookkeeper.client.LedgerHandle newlh =
                    newbk.openLedger(id,
                                org.apache.bk_v4_2_0.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                "foobar".getBytes());
            return new Ledger420(newbk, newlh);
        }

        long getId() {
            return lh.getId();
        }

        void write100() throws Exception {
            for (int i = 0; i < 100; i++) {
                lh.addEntry(ENTRYDATA);
            }
        }

        long readAll() throws Exception {
            long count = 0;
            Enumeration<org.apache.bk_v4_2_0.bookkeeper.client.LedgerEntry> entries =
                    lh.readEntries(0, lh.getLastAddConfirmed());
            while (entries.hasMoreElements()) {
                assertTrue("entry data doesn't match",
                           Arrays.equals(entries.nextElement().getEntry(), ENTRYDATA));
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
     * Current verion classes.
     */
    static class ServerCurrent {
        org.apache.bookkeeper.conf.ServerConfiguration conf;
        org.apache.bookkeeper.proto.BookieServer server = null;

        ServerCurrent(File journalDir, File ledgerDir, int port,
                boolean useHostNameAsBookieID) throws Exception {
            conf = TestBKConfiguration.newServerConfiguration();
            conf.setAllowEphemeralPorts(false);
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
            org.apache.bookkeeper.client.BookKeeper newbk =
                    new org.apache.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bookkeeper.client.LedgerHandle newlh =
                    newbk.createLedger(1, 1,
                                     org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                     "foobar".getBytes());
            return new LedgerCurrent(newbk, newlh);
        }

        static LedgerCurrent openLedger(long id) throws Exception {
            org.apache.bookkeeper.client.BookKeeper newbk =
                    new org.apache.bookkeeper.client.BookKeeper(zkUtil.getZooKeeperConnectString());
            org.apache.bookkeeper.client.LedgerHandle newlh =
                    newbk.openLedger(id,
                                org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                "foobar".getBytes());
            return new LedgerCurrent(newbk, newlh);
        }

        static LedgerCurrent openLedger(long id, ClientConfiguration conf) throws Exception {
            conf.setZkServers(zkUtil.getZooKeeperConnectString());
            org.apache.bookkeeper.client.BookKeeper newbk =
                    new org.apache.bookkeeper.client.BookKeeper(conf);
            org.apache.bookkeeper.client.LedgerHandle newlh =
                    newbk.openLedger(id,
                                   org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32,
                                "foobar".getBytes());
            return new LedgerCurrent(newbk, newlh);
        }

        long getId() {
            return lh.getId();
        }

        void write100() throws Exception {
            for (int i = 0; i < 100; i++) {
                lh.addEntry(ENTRYDATA);
            }
        }

        long readAll() throws Exception {
            long count = 0;
            Enumeration<org.apache.bookkeeper.client.LedgerEntry> entries =
                    lh.readEntries(0, lh.getLastAddConfirmed());
            while (entries.hasMoreElements()) {
                assertTrue("entry data doesn't match",
                           Arrays.equals(entries.nextElement().getEntry(), ENTRYDATA));
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
    @Test
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
        BookKeeperAdmin.format(currentServer.conf,
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

}
