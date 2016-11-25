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
package org.apache.bookkeeper.sasl;

import java.io.File;
import java.io.FileWriter;
import java.net.InetAddress;
import java.util.Arrays;
import org.apache.bookkeeper.client.*;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import javax.security.auth.login.Configuration;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.After;
import org.junit.AfterClass;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class MD5DigestBookKeeperTest extends BookKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(MD5DigestBookKeeperTest.class);

    private static final byte[] PASSWD = "testPasswd".getBytes();
    private static final byte[] ENTRY = "TestEntry".getBytes();

    @Rule
    public TemporaryFolder jaasWorkDir = new TemporaryFolder();

    @Before
    public void setupJaas() throws Exception {

        File jaas_file = new File(jaasWorkDir.getRoot(), "jaas.conf");
        try (FileWriter writer = new FileWriter(jaas_file)) {
            writer.write(""
                + "Bookie {\n"
                + "       org.apache.zookeeper.server.auth.DigestLoginModule required\n"
                + "       user_standarduser=\"testpwd_client\"\n"
                + "       user_systemuser=\"testpwd_auditor\";\n"
                + "};\n"
                + "\n"
                + "Auditor {\n"
                + "       org.apache.zookeeper.server.auth.DigestLoginModule required\n"
                + "       username=\"systemuser\"\n"
                + "       password=\"testpwd_auditor\";\n"
                + "};\n"
                + "\n"
                + "BookKeeper {\n"
                + "       org.apache.zookeeper.server.auth.DigestLoginModule required\n"
                + "       username=\"standarduser\"\n"
                + "       password=\"testpwd_client\";\n"
                + "};\n"
            );

        }

        System.setProperty("java.security.auth.login.config", jaas_file.getAbsolutePath());
        javax.security.auth.login.Configuration.getConfiguration().refresh();

    }

    @After
    public void disableJaas() {
        System.clearProperty("java.security.auth.login.config");
    }

    public MD5DigestBookKeeperTest() {
        super(0); // start them later when auth providers are configured
    }

    // we pass in ledgerId because the method may throw exceptions
    private void connectAndWriteToBookie(ClientConfiguration conf, AtomicLong ledgerWritten)
        throws Exception {
        LOG.info("Connecting to bookie");
        BookKeeper bkc = new BookKeeper(conf, zkc);
        LedgerHandle l = bkc.createLedger(1, 1, DigestType.CRC32,
            PASSWD);
        ledgerWritten.set(l.getId());
        l.addEntry(ENTRY);
        l.close();
        bkc.close();
    }

    /**
     * check if the entry exists. Restart the bookie to allow access
     */
    private int entryCount(long ledgerId, ServerConfiguration bookieConf,
        ClientConfiguration clientConf) throws Exception {
        LOG.info("Counting entries in {}", ledgerId);
        for (ServerConfiguration conf : bsConfs) {
            bookieConf.setBookieAuthProviderFactoryClass(
                SASLBookieAuthProviderFactory.class.getName());
        }
        clientConf.setClientAuthProviderFactoryClass(
            SASLClientProviderFactory.class.getName());

        restartBookies();

        BookKeeper bkc = new BookKeeper(clientConf, zkc);
        LedgerHandle lh = bkc.openLedger(ledgerId, DigestType.CRC32,
            PASSWD);
        if (lh.getLastAddConfirmed() < 0) {
            return 0;
        }
        Enumeration<LedgerEntry> e = lh.readEntries(0, lh.getLastAddConfirmed());
        int count = 0;
        while (e.hasMoreElements()) {
            count++;
            assertTrue("Should match what we wrote",
                Arrays.equals(e.nextElement().getEntry(), ENTRY));
        }
        return count;
    }

    /**
     * Test an connection will authorize with a single message to the server and a single response.
     */
//    @Test(timeout=30000)
    @Test
    public void testSingleMessageAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
            SASLBookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
            SASLClientProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        connectAndWriteToBookie(clientConf, ledgerId); // should succeed

        assertFalse(ledgerId.get() == -1);
        assertEquals("Should have entry", 1, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    BookieServer startAndStoreBookie(ServerConfiguration conf) throws Exception {
        bsConfs.add(conf);
        BookieServer s = startBookie(conf);
        bs.add(s);
        return s;
    }

    @AfterClass
    public static void resetJAAS() {
        System.clearProperty("java.security.auth.login.config");
        Configuration.getConfiguration().refresh();
    }
}
