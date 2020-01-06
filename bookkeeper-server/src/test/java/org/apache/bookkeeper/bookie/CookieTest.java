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

import static org.apache.bookkeeper.bookie.UpgradeTest.initV1JournalDirectory;
import static org.apache.bookkeeper.bookie.UpgradeTest.initV1LedgerDirectory;
import static org.apache.bookkeeper.bookie.UpgradeTest.initV2JournalDirectory;
import static org.apache.bookkeeper.bookie.UpgradeTest.initV2LedgerDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.bookkeeper.bookie.BookieException.InvalidCookieException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.PortManager;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

/**
 * Test cookies.
 */
public class CookieTest extends BookKeeperClusterTestCase {
    final int bookiePort = PortManager.nextFreePort();

    public CookieTest() {
        super(0);
    }

    private String newDirectory() throws IOException {
        return newDirectory(true);
    }

    private String newDirectory(boolean createCurDir) throws IOException {
        File d = createTempDir("cookie", "tmpdir");
        if (createCurDir) {
            new File(d, "current").mkdirs();
        }
        return d.getPath();
    }

    MetadataBookieDriver metadataBookieDriver;
    RegistrationManager rm;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        this.metadataBookieDriver = MetadataDrivers.getBookieDriver(
            URI.create(baseConf.getMetadataServiceUri()));
        this.metadataBookieDriver.initialize(baseConf, () -> {}, NullStatsLogger.INSTANCE);
        this.rm = metadataBookieDriver.getRegistrationManager();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (metadataBookieDriver != null) {
            metadataBookieDriver.close();
        }
    }

    /**
     * Test starting bookie with clean state.
     */
    @Test
    public void testCleanStart() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(newDirectory(false))
            .setLedgerDirNames(new String[] { newDirectory(false) })
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        try {
            Bookie b = new BookieImpl(conf);
        } catch (Exception e) {
            fail("Should not reach here.");
        }
    }

    /**
     * Test that if a zookeeper cookie
     * is different to a local cookie, the bookie
     * will fail to start.
     */
    @Test
    public void testBadJournalCookie() throws Exception {
        ServerConfiguration conf1 = TestBKConfiguration.newServerConfiguration()
            .setJournalDirName(newDirectory())
            .setLedgerDirNames(new String[] { newDirectory() })
            .setBookiePort(bookiePort);
        Cookie.Builder cookieBuilder = Cookie.generateCookie(conf1);
        Cookie c = cookieBuilder.build();
        c.writeToRegistrationManager(rm, conf1, Version.NEW);

        String journalDir = newDirectory();
        String ledgerDir = newDirectory();
        ServerConfiguration conf2 = TestBKConfiguration.newServerConfiguration();
        conf2.setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir })
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        Cookie.Builder cookieBuilder2 = Cookie.generateCookie(conf2);
        Cookie c2 = cookieBuilder2.build();
        c2.writeToDirectory(new File(journalDir, "current"));
        c2.writeToDirectory(new File(ledgerDir, "current"));

        try {
            Bookie b = new BookieImpl(conf2);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test that if a directory is removed from
     * the configuration, the bookie will fail to
     * start.
     */
    @Test
    public void testDirectoryMissing() throws Exception {
        String[] ledgerDirs = new String[] {
            newDirectory(), newDirectory(), newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir)
            .setLedgerDirNames(ledgerDirs)
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setLedgerDirNames(new String[] { ledgerDirs[0], ledgerDirs[1] });
        try {
            Bookie b2 = new BookieImpl(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }

        conf.setJournalDirName(newDirectory()).setLedgerDirNames(ledgerDirs);
        try {
            Bookie b2 = new BookieImpl(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }

        conf.setJournalDirName(journalDir);
        b = new BookieImpl(conf);
        b.start();
        b.shutdown();
    }

    /**
     * Test that if a cookie is missing from a journal directory
     * the bookie will fail to start.
     */
    @Test
    public void testCookieMissingOnJournalDir() throws Exception {
        String[] ledgerDirs = new String[] {
            newDirectory(), newDirectory(), newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir)
            .setLedgerDirNames(ledgerDirs)
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();

        File cookieFile =
            new File(BookieImpl.getCurrentDirectory(new File(journalDir)), BookKeeperConstants.VERSION_FILENAME);
        assertTrue(cookieFile.delete());
        try {
            new BookieImpl(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test that if a cookie is missing from a journal directory
     * the bookie will fail to start.
     */
    @Test
    public void testCookieMissingOnLedgerDir() throws Exception {
        String[] ledgerDirs = new String[] {
            newDirectory(), newDirectory(), newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir)
            .setLedgerDirNames(ledgerDirs)
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();

        File cookieFile =
            new File(BookieImpl.getCurrentDirectory(new File(ledgerDirs[0])), BookKeeperConstants.VERSION_FILENAME);
        assertTrue(cookieFile.delete());
        try {
            new BookieImpl(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test that if a directory is added to a
     * preexisting bookie, the bookie will fail
     * to start.
     */
    @Test
    public void testDirectoryAdded() throws Exception {
        String ledgerDir0 = newDirectory();
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir0 })
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setLedgerDirNames(new String[] { ledgerDir0, newDirectory() });
        try {
            Bookie b2 = new BookieImpl(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }

        conf.setLedgerDirNames(new String[] { ledgerDir0 });
        b = new BookieImpl(conf);
        b.start();
        b.shutdown();
    }

    /**
     * Test that if a directory is added to an existing bookie, and
     * allowStorageExpansion option is true, the bookie should come online.
     */
    @Test
    public void testStorageExpansionOption() throws Exception {
        String ledgerDir0 = newDirectory();
        String indexDir0 = newDirectory();
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir0 })
            .setIndexDirName(new String[] { indexDir0 })
            .setBookiePort(bookiePort)
            .setAllowStorageExpansion(true)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        BookieImpl b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();
        b = null;

        // add a few additional ledger dirs
        String[] lPaths = new String[] {ledgerDir0, newDirectory(), newDirectory()};
        Set<String> configuredLedgerDirs =  Sets.newHashSet(lPaths);
        conf.setLedgerDirNames(lPaths);

        // add an extra index dir
        String[] iPaths = new String[] {indexDir0, newDirectory()};
        Set<String> configuredIndexDirs =  Sets.newHashSet(iPaths);
        conf.setIndexDirName(iPaths);

        try {
            b = new BookieImpl(conf);
        } catch (BookieException.InvalidCookieException ice) {
            fail("Should have been able to start the bookie");
        }

        List<File> l = b.getLedgerDirsManager().getAllLedgerDirs();
        HashSet<String> bookieLedgerDirs = Sets.newHashSet();
        for (File f : l) {
            // Using the parent path because the bookie creates a 'current'
            // dir under the ledger dir user provides
            bookieLedgerDirs.add(f.getParent());
        }
        assertTrue("Configured ledger dirs: " + configuredLedgerDirs + " doesn't match bookie's ledger dirs: "
                   + bookieLedgerDirs,
                   configuredLedgerDirs.equals(bookieLedgerDirs));

        l = b.getIndexDirsManager().getAllLedgerDirs();
        HashSet<String> bookieIndexDirs = Sets.newHashSet();
        for (File f : l) {
            bookieIndexDirs.add(f.getParent());
        }
        assertTrue("Configured Index dirs: " + configuredIndexDirs + " doesn't match bookie's index dirs: "
                   + bookieIndexDirs,
                   configuredIndexDirs.equals(bookieIndexDirs));

        b.shutdown();

        // Make sure that substituting an older ledger directory
        // is not allowed.
        String[] lPaths2 = new String[] { lPaths[0], lPaths[1], newDirectory() };
        conf.setLedgerDirNames(lPaths2);
        try {
            b = new BookieImpl(conf);
            fail("Should not have been able to start the bookie");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behavior
        }

        // Finally make sure that not including the older ledger directories
        // is not allowed. Remove one of the older ledger dirs
        lPaths2 = new String[] { lPaths[0], lPaths[1] };
        conf.setLedgerDirNames(lPaths2);
        try {
            b = new BookieImpl(conf);
            fail("Should not have been able to start the bookie");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behavior
        }
    }

    /**
     * Test that adding of a non-empty directory is not allowed
     * even when allowStorageExpansion option is true.
     */
    @Test
    public void testNonEmptyDirAddWithStorageExpansionOption() throws Exception {
        String ledgerDir0 = newDirectory();
        String indexDir0 = newDirectory();
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir0 })
            .setIndexDirName(new String[] { indexDir0 })
            .setBookiePort(bookiePort)
            .setAllowStorageExpansion(true)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();
        b = null;

        // add an additional ledger dir
        String[] lPaths = new String[] {ledgerDir0, newDirectory()};
        conf.setLedgerDirNames(lPaths);

        // create a file to make the dir non-empty
        File currentDir = BookieImpl.getCurrentDirectory(new File(lPaths[1]));
        new File(currentDir, "foo").createNewFile();
        assertTrue(currentDir.list().length == 1);

        try {
            b = new BookieImpl(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behavior
        }

        // Now test with a non-empty index dir
        String[] iPaths = new String[] {indexDir0, newDirectory()};
        conf.setIndexDirName(iPaths);

        // create a dir to make it non-empty
        currentDir = BookieImpl.getCurrentDirectory(new File(iPaths[1]));
        new File(currentDir, "bar").mkdirs();
        assertTrue(currentDir.list().length == 1);

        try {
            b = new BookieImpl(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behavior
        }
    }

    /**
     * Test that if a directory's contents
     * are emptied, the bookie will fail to start.
     */
    @Test
    public void testDirectoryCleared() throws Exception {
        String ledgerDir0 = newDirectory();
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir0 , newDirectory() })
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();

        FileUtils.deleteDirectory(new File(ledgerDir0));
        try {
            Bookie b2 = new BookieImpl(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test that if a bookie's port is changed
     * the bookie will fail to start.
     */
    @Test
    public void testBookiePortChanged() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(newDirectory())
            .setLedgerDirNames(new String[] { newDirectory() , newDirectory() })
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setBookiePort(3182);
        try {
            b = new BookieImpl(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test that if a bookie tries to start
     * with the address of a bookie which has already
     * existed in the system, then the bookie will fail
     * to start.
     */
    @Test
    public void testNewBookieStartingWithAnotherBookiesPort() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(newDirectory())
            .setLedgerDirNames(new String[] { newDirectory() , newDirectory() })
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();

        conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(newDirectory())
            .setLedgerDirNames(new String[] { newDirectory() , newDirectory() })
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        try {
            b = new BookieImpl(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test Cookie verification with format.
     */
    @Test
    public void testVerifyCookieWithFormat() throws Exception {
        ServerConfiguration adminConf = new ServerConfiguration();
        adminConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        adminConf.setProperty("bookkeeper.format", true);
        // Format the BK Metadata and generate INSTANCEID
        BookKeeperAdmin.format(adminConf, false, true);

        ServerConfiguration bookieConf = TestBKConfiguration.newServerConfiguration();
        bookieConf.setJournalDirName(newDirectory(false))
            .setLedgerDirNames(new String[] { newDirectory(false) })
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        // Bookie should start successfully for fresh env.
        new BookieImpl(bookieConf);

        // Format metadata one more time.
        BookKeeperAdmin.format(adminConf, false, true);
        try {
            new BookieImpl(bookieConf);
            fail("Bookie should not start with previous instance id.");
        } catch (BookieException.InvalidCookieException e) {
            assertTrue(
                    "Bookie startup should fail because of invalid instance id",
                    e.getMessage().contains("instanceId"));
        }

        // Now format the Bookie and restart.
        BookieImpl.format(bookieConf, false, true);
        // After bookie format bookie should be able to start again.
        new BookieImpl(bookieConf);
    }

    /**
     * Test that if a bookie is started with directories with
     * version 2 data, that it will fail to start (it needs upgrade).
     */
    @Test
    public void testV2data() throws Exception {
        File journalDir = initV2JournalDirectory(createTempDir("bookie", "journal"));
        File ledgerDir = initV2LedgerDirectory(createTempDir("bookie", "ledger"));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        try {
            Bookie b = new BookieImpl(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
            assertTrue("wrong exception", ice.getCause().getMessage().contains("upgrade needed"));
        }
    }

    /**
     * Test that if a bookie is started with directories with
     * version 1 data, that it will fail to start (it needs upgrade).
     */
    @Test
    public void testV1data() throws Exception {
        File journalDir = initV1JournalDirectory(createTempDir("bookie", "journal"));
        File ledgerDir = initV1LedgerDirectory(createTempDir("bookie", "ledger"));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[]{ledgerDir.getPath()})
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        try {
            Bookie b = new BookieImpl(conf);
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
    @Test
    public void testRestartWithHostNameAsBookieID() throws Exception {
        String[] ledgerDirs = new String[] { newDirectory(), newDirectory(),
                newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir)
            .setLedgerDirNames(ledgerDirs)
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setUseHostNameAsBookieID(true);
        try {
            new BookieImpl(conf);
            fail("Should not start a bookie with hostname if the bookie has been started with an ip");
        } catch (InvalidCookieException e) {
            // expected
        }
    }

    /**
     * Test restart bookie with new advertisedAddress, which had cookie generated with ip.
     */
    @Test
    public void testRestartWithAdvertisedAddressAsBookieID() throws Exception {
        String[] ledgerDirs = new String[] { newDirectory(), newDirectory(),
                newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir)
            .setLedgerDirNames(ledgerDirs)
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        conf.setUseHostNameAsBookieID(false);
        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setAdvertisedAddress("unknown");
        try {
            new BookieImpl(conf);
            fail("Should not start a bookie with ip if the bookie has been started with an ip");
        } catch (InvalidCookieException e) {
            // expected
        }
    }

    /**
     * Test restart bookie with useHostNameAsBookieID=false, which had cookie generated
     * with hostname.
     */
    @Test
    public void testRestartWithIpAddressAsBookieID() throws Exception {
        String[] ledgerDirs = new String[] { newDirectory(), newDirectory(),
                newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir).setLedgerDirNames(ledgerDirs)
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        conf.setUseHostNameAsBookieID(true);
        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setUseHostNameAsBookieID(false);
        try {
            new BookieImpl(conf);
            fail("Should not start a bookie with ip if the bookie has been started with an ip");
        } catch (InvalidCookieException e) {
            // expected
        }
    }

    /**
     * Test old version bookie starts with the cookies generated by new version
     * (with useHostNameAsBookieID=true).
     */
    @Test
    public void testV2dataWithHostNameAsBookieID() throws Exception {
        File journalDir = initV2JournalDirectory(createTempDir("bookie", "journal"));
        File ledgerDir = initV2LedgerDirectory(createTempDir("bookie", "ledger"));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        try {
            conf.setUseHostNameAsBookieID(true);
            new BookieImpl(conf);
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
    @Test
    public void testWriteToZooKeeper() throws Exception {
        String[] ledgerDirs = new String[] { newDirectory(), newDirectory(), newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir)
            .setLedgerDirNames(ledgerDirs)
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();
        Versioned<Cookie> zkCookie = Cookie.readFromRegistrationManager(rm, conf);
        Version version1 = zkCookie.getVersion();
        assertTrue("Invalid type expected ZkVersion type",
            version1 instanceof LongVersion);
        LongVersion zkVersion1 = (LongVersion) version1;
        Cookie cookie = zkCookie.getValue();
        cookie.writeToRegistrationManager(rm, conf, version1);

        zkCookie = Cookie.readFromRegistrationManager(rm, conf);
        Version version2 = zkCookie.getVersion();
        assertTrue("Invalid type expected ZkVersion type",
            version2 instanceof LongVersion);
        LongVersion zkVersion2 = (LongVersion) version2;
        assertEquals("Version mismatches!",
            zkVersion1.getLongVersion() + 1, zkVersion2.getLongVersion());
    }

    /**
     * Test delete cookie.
     */
    @Test
    public void testDeleteFromZooKeeper() throws Exception {
        String[] ledgerDirs = new String[] { newDirectory(), newDirectory(), newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir)
            .setLedgerDirNames(ledgerDirs)
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        Bookie b = new BookieImpl(conf); // should work fine
        b.start();
        b.shutdown();
        Versioned<Cookie> zkCookie = Cookie.readFromRegistrationManager(rm, conf);
        Cookie cookie = zkCookie.getValue();
        cookie.deleteFromRegistrationManager(rm, conf, zkCookie.getVersion());
    }
}
