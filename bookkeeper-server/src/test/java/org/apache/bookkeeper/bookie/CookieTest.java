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

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.util.IOUtils;

import static org.apache.bookkeeper.bookie.UpgradeTest.newV1JournalDirectory;
import static org.apache.bookkeeper.bookie.UpgradeTest.newV1LedgerDirectory;
import static org.apache.bookkeeper.bookie.UpgradeTest.newV2JournalDirectory;
import static org.apache.bookkeeper.bookie.UpgradeTest.newV2LedgerDirectory;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class CookieTest extends BookKeeperClusterTestCase {
    final int bookiePort = PortManager.nextFreePort();

    public CookieTest() {
        super(0);
    }

    private String newDirectory() throws IOException {
        return newDirectory(true);
    }

    private String newDirectory(boolean createCurDir) throws IOException {
        File d = IOUtils.createTempDir("cookie", "tmpdir");
        if (createCurDir) {
            new File(d, "current").mkdirs();
        }
        tmpDirs.add(d);
        return d.getPath();
    }

    /**
     * Test starting bookie with clean state.
     */
    @Test(timeout=60000)
    public void testCleanStart() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(newDirectory(false))
            .setLedgerDirNames(new String[] { newDirectory(false) })
            .setBookiePort(bookiePort);
        try {
            Bookie b = new Bookie(conf);
        } catch (Exception e) {
            fail("Should not reach here.");
        }
    }

    /**
     * Test that if a zookeeper cookie
     * is different to a local cookie, the bookie
     * will fail to start
     */
    @Test(timeout=60000)
    public void testBadJournalCookie() throws Exception {
        ServerConfiguration conf1 = TestBKConfiguration.newServerConfiguration()
            .setJournalDirName(newDirectory())
            .setLedgerDirNames(new String[] { newDirectory() })
            .setBookiePort(bookiePort);
        Cookie.Builder cookieBuilder = Cookie.generateCookie(conf1);
        Cookie c = cookieBuilder.build();
        c.writeToZooKeeper(zkc, conf1, Version.NEW);

        String journalDir = newDirectory();
        String ledgerDir = newDirectory();
        ServerConfiguration conf2 = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir })
            .setBookiePort(bookiePort);
        Cookie.Builder cookieBuilder2 = Cookie.generateCookie(conf2);
        Cookie c2 = cookieBuilder2.build();
        c2.writeToDirectory(new File(journalDir, "current"));
        c2.writeToDirectory(new File(ledgerDir, "current"));

        try {
            Bookie b = new Bookie(conf2);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test that if a directory is removed from
     * the configuration, the bookie will fail to
     * start
     */
    @Test(timeout=60000)
    public void testDirectoryMissing() throws Exception {
        String[] ledgerDirs = new String[] {
            newDirectory(), newDirectory(), newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(journalDir)
            .setLedgerDirNames(ledgerDirs)
            .setBookiePort(bookiePort);

        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setLedgerDirNames(new String[] { ledgerDirs[0], ledgerDirs[1] });
        try {
            Bookie b2 = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }

        conf.setJournalDirName(newDirectory()).setLedgerDirNames(ledgerDirs);
        try {
            Bookie b2 = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }

        conf.setJournalDirName(journalDir);
        b = new Bookie(conf);
        b.start();
        b.shutdown();
    }

    /**
     * Test that if a directory is added to a
     * preexisting bookie, the bookie will fail
     * to start
     */
    @Test(timeout=60000)
    public void testDirectoryAdded() throws Exception {
        String ledgerDir0 = newDirectory();
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir0 })
            .setBookiePort(bookiePort);

        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setLedgerDirNames(new String[] { ledgerDir0, newDirectory() });
        try {
            Bookie b2 = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }

        conf.setLedgerDirNames(new String[] { ledgerDir0 });
        b = new Bookie(conf);
        b.start();
        b.shutdown();
    }

    /**
     * Test that if a directory's contents
     * are emptied, the bookie will fail to start
     */
    @Test(timeout=60000)
    public void testDirectoryCleared() throws Exception {
        String ledgerDir0 = newDirectory();
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir0 , newDirectory() })
            .setBookiePort(bookiePort);

        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();

        FileUtils.deleteDirectory(new File(ledgerDir0));
        try {
            Bookie b2 = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test that if a bookie's port is changed
     * the bookie will fail to start
     */
    @Test(timeout=60000)
    public void testBookiePortChanged() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(newDirectory())
            .setLedgerDirNames(new String[] { newDirectory() , newDirectory() })
            .setBookiePort(bookiePort);
        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setBookiePort(3182);
        try {
            b = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test that if a bookie tries to start
     * with the address of a bookie which has already
     * existed in the system, then the bookie will fail
     * to start
     */
    @Test(timeout=60000)
    public void testNewBookieStartingWithAnotherBookiesPort() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(newDirectory())
            .setLedgerDirNames(new String[] { newDirectory() , newDirectory() })
            .setBookiePort(bookiePort);
        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();

        conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(newDirectory())
            .setLedgerDirNames(new String[] { newDirectory() , newDirectory() })
            .setBookiePort(bookiePort);
        try {
            b = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /*
     * Test Cookie verification with format.
     */
    @Test(timeout=60000)
    public void testVerifyCookieWithFormat() throws Exception {
        ClientConfiguration adminConf = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());

        adminConf.setProperty("bookkeeper.format", true);
        // Format the BK Metadata and generate INSTANCEID
        BookKeeperAdmin.format(adminConf, false, true);

        ServerConfiguration bookieConf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setJournalDirName(newDirectory(false))
                .setLedgerDirNames(new String[] { newDirectory(false) })
                .setBookiePort(bookiePort);
        // Bookie should start successfully for fresh env.
        new Bookie(bookieConf);

        // Format metadata one more time.
        BookKeeperAdmin.format(adminConf, false, true);
        try {
            new Bookie(bookieConf);
            fail("Bookie should not start with previous instance id.");
        } catch (BookieException.InvalidCookieException e) {
            assertTrue(
                    "Bookie startup should fail because of invalid instance id",
                    e.getMessage().contains("instanceId"));
        }

        // Now format the Bookie and restart.
        Bookie.format(bookieConf, false, true);
        // After bookie format bookie should be able to start again.
        new Bookie(bookieConf);
    }

    /**
     * Test that if a bookie is started with directories with
     * version 2 data, that it will fail to start (it needs upgrade)
     */
    @Test(timeout=60000)
    public void testV2data() throws Exception {
        File journalDir = newV2JournalDirectory();
        tmpDirs.add(journalDir);
        File ledgerDir = newV2LedgerDirectory();
        tmpDirs.add(ledgerDir);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setBookiePort(bookiePort);
        try {
            Bookie b = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
            assertTrue("wrong exception", ice.getCause().getMessage().contains("upgrade needed"));
        }
    }

    /**
     * Test that if a bookie is started with directories with
     * version 1 data, that it will fail to start (it needs upgrade)
     */
    @Test(timeout=60000)
    public void testV1data() throws Exception {
        File journalDir = newV1JournalDirectory();
        tmpDirs.add(journalDir);
        File ledgerDir = newV1LedgerDirectory();
        tmpDirs.add(ledgerDir);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[]{ledgerDir.getPath()})
            .setBookiePort(bookiePort);
        try {
            Bookie b = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
            assertTrue("wrong exception", ice.getCause().getMessage().contains("upgrade needed"));
        }
    }

    /**
     * Test restart bookie with useHostNameAsBookieID=true, which had cookie generated
     * with ipaddress.
     */
    @Test(timeout = 60000)
    public void testRestartWithHostNameAsBookieID() throws Exception {
        String[] ledgerDirs = new String[] { newDirectory(), newDirectory(),
                newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setJournalDirName(journalDir).setLedgerDirNames(ledgerDirs)
                .setBookiePort(bookiePort);
        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setUseHostNameAsBookieID(true);
        b = new Bookie(conf);
        b.start();
        assertTrue("Fails to recognize bookie which was started with IPAddr as ID", !conf.getUseHostNameAsBookieID());
        b.shutdown();
    }

    /**
     * Test restart bookie with useHostNameAsBookieID=false, which had cookie generated
     * with hostname.
     */
    @Test(timeout = 60000)
    public void testRestartWithIpAddressAsBookieID() throws Exception {
        String[] ledgerDirs = new String[] { newDirectory(), newDirectory(),
                newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setJournalDirName(journalDir).setLedgerDirNames(ledgerDirs)
                .setBookiePort(bookiePort);
        conf.setUseHostNameAsBookieID(true);
        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setUseHostNameAsBookieID(false);
        b = new Bookie(conf);
        b.start();
        assertTrue("Fails to recognize bookie which was started with HostName as ID", conf.getUseHostNameAsBookieID());
        b.shutdown();
    }

    /**
     * Test old version bookie starts with the cookies generated by new version
     * (with useHostNameAsBookieID=true)
     */
    @Test(timeout = 60000)
    public void testV2dataWithHostNameAsBookieID() throws Exception {
        File journalDir = newV2JournalDirectory();
        tmpDirs.add(journalDir);
        File ledgerDir = newV2LedgerDirectory();
        tmpDirs.add(ledgerDir);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setJournalDirName(journalDir.getPath())
                .setLedgerDirNames(new String[] { ledgerDir.getPath() })
                .setBookiePort(bookiePort);
        try {
            conf.setUseHostNameAsBookieID(true);
            new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
            assertTrue("wrong exception",
                    ice.getCause().getMessage().contains("upgrade needed"));
        }
    }

    /**
     * Test write cookie multiple times.
     */
    @Test(timeout = 60000)
    public void testWriteToZooKeeper() throws Exception {
        String[] ledgerDirs = new String[] { newDirectory(), newDirectory(), newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString()).setJournalDirName(journalDir)
                .setLedgerDirNames(ledgerDirs).setBookiePort(bookiePort);
        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();
        Versioned<Cookie> zkCookie = Cookie.readFromZooKeeper(zkc, conf);
        Version version1 = zkCookie.getVersion();
        Assert.assertTrue("Invalid type expected ZkVersion type", version1 instanceof ZkVersion);
        ZkVersion zkVersion1 = (ZkVersion) version1;
        Cookie cookie = zkCookie.getValue();
        cookie.writeToZooKeeper(zkc, conf, version1);

        zkCookie = Cookie.readFromZooKeeper(zkc, conf);
        Version version2 = zkCookie.getVersion();
        Assert.assertTrue("Invalid type expected ZkVersion type", version2 instanceof ZkVersion);
        ZkVersion zkVersion2 = (ZkVersion) version2;
        Assert.assertEquals("Version mismatches!", zkVersion1.getZnodeVersion() + 1, zkVersion2.getZnodeVersion());
    }

    /**
     * Test delete cookie.
     */
    @Test(timeout = 60000)
    public void testDeleteFromZooKeeper() throws Exception {
        String[] ledgerDirs = new String[] { newDirectory(), newDirectory(), newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString()).setJournalDirName(journalDir)
                .setLedgerDirNames(ledgerDirs).setBookiePort(bookiePort);
        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();
        Versioned<Cookie> zkCookie = Cookie.readFromZooKeeper(zkc, conf);
        Cookie cookie = zkCookie.getValue();
        cookie.deleteFromZooKeeper(zkc, conf, zkCookie.getVersion());
    }
}
