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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import javax.security.auth.login.Configuration;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKUnauthorizedAccessException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GSSAPI tests.
 */
public class GSSAPIBookKeeperTest extends BookKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(GSSAPIBookKeeperTest.class);

    private static final byte[] PASSWD = "testPasswd".getBytes();
    private static final byte[] ENTRY = "TestEntry".getBytes();

    private static MiniKdc kdc;
    private static Properties conf;

    private static final String non_default_sasl_service_name = "non_default_servicename";

    @ClassRule
    public static TemporaryFolder kdcDir = new TemporaryFolder();

    @ClassRule
    public static TemporaryFolder kerberosWorkDir = new TemporaryFolder();

    @BeforeClass
    public static void startMiniKdc() throws Exception {

        conf = MiniKdc.createConf();
        kdc = new MiniKdc(conf, kdcDir.getRoot());
        kdc.start();

        // this is just to calculate "localhostName" the same way the bookie does
        ServerConfiguration bookieConf = TestBKConfiguration.newServerConfiguration();
        bookieConf.setUseHostNameAsBookieID(true);
        String localhostName = Bookie.getBookieAddress(bookieConf).getHostName();

        String principalServerNoRealm = non_default_sasl_service_name + "/" + localhostName;
        String principalServer = non_default_sasl_service_name + "/" + localhostName + "@" + kdc.getRealm();
        LOG.info("principalServer: " + principalServer);
        String principalClientNoRealm = "bookkeeperclient/" + localhostName;
        String principalClient = principalClientNoRealm + "@" + kdc.getRealm();
        LOG.info("principalClient: " + principalClient);

        File keytabClient = new File(kerberosWorkDir.getRoot(), "bookkeeperclient.keytab");
        kdc.createPrincipal(keytabClient, principalClientNoRealm);

        File keytabServer = new File(kerberosWorkDir.getRoot(), "bookkeeperserver.keytab");
        kdc.createPrincipal(keytabServer, principalServerNoRealm);

        File jaasFile = new File(kerberosWorkDir.getRoot(), "jaas.conf");
        try (FileWriter writer = new FileWriter(jaasFile)) {
            writer.write("\n"
                + "Bookie {\n"
                + "  com.sun.security.auth.module.Krb5LoginModule required debug=true\n"
                + "  useKeyTab=true\n"
                + "  keyTab=\"" + keytabServer.getAbsolutePath() + "\n"
                + "  storeKey=true\n"
                + "  useTicketCache=false\n" // won't test useTicketCache=true on JUnit tests
                + "  principal=\"" + principalServer + "\";\n"
                + "};\n"
                + "\n"
                + "\n"
                + "\n"
                + "BookKeeper {\n"
                + "  com.sun.security.auth.module.Krb5LoginModule required debug=true\n"
                + "  useKeyTab=true\n"
                + "  keyTab=\"" + keytabClient.getAbsolutePath() + "\n"
                + "  storeKey=true\n"
                + "  useTicketCache=false\n"
                + "  principal=\"" + principalClient + "\";\n"
                + "};\n"
            );

        }

        File krb5file = new File(kerberosWorkDir.getRoot(), "krb5.conf");
        try (FileWriter writer = new FileWriter(krb5file)) {
            String conf = "[libdefaults]\n"
                + " default_realm = " + kdc.getRealm() + "\n"
                + " udp_preference_limit = 1\n" // force use TCP
                + "\n"
                + "\n"
                + "[realms]\n"
                + " " + kdc.getRealm() + "  = {\n"
                + "  kdc = " + kdc.getHost() + ":" + kdc.getPort() + "\n"
                + " }";
            writer.write(conf);
            LOG.info("krb5.conf:\n" + conf);
        }

        System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath());
        System.setProperty("java.security.krb5.conf", krb5file.getAbsolutePath());
        javax.security.auth.login.Configuration.getConfiguration().refresh();

    }

    @AfterClass
    public static void stopMiniKdc() {
        System.clearProperty("java.security.auth.login.config");
        System.clearProperty("java.security.krb5.conf");
        if (kdc != null) {
            kdc.stop();
        }
    }

    public GSSAPIBookKeeperTest() {
        super(0); // start them later when auth providers are configured
    }

    // we pass in ledgerId because the method may throw exceptions
    private void connectAndWriteToBookie(ClientConfiguration conf, AtomicLong ledgerWritten)
        throws BKException, InterruptedException, IOException, KeeperException {
        LOG.info("Connecting to bookie");
        try (BookKeeper bkc = new BookKeeper(conf, zkc)) {
            LedgerHandle l = bkc.createLedger(1, 1, DigestType.CRC32,
                PASSWD);
            ledgerWritten.set(l.getId());
            l.addEntry(ENTRY);
            l.close();
        }
    }

    /**
     * check if the entry exists. Restart the bookie to allow access
     */
    private int entryCount(long ledgerId, ClientConfiguration clientConf)
            throws Exception {
        LOG.info("Counting entries in {}", ledgerId);
        for (ServerConfiguration conf : bsConfs) {
            conf.setUseHostNameAsBookieID(true);
            conf.setBookieAuthProviderFactoryClass(
                SASLBookieAuthProviderFactory.class.getName());
        }
        clientConf.setClientAuthProviderFactoryClass(
            SASLClientProviderFactory.class.getName());

        restartBookies();

        try (BookKeeper bkc = new BookKeeper(clientConf, zkc);
            LedgerHandle lh = bkc.openLedger(ledgerId, DigestType.CRC32,
                PASSWD)) {
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
    }

    /**
     * Test an connection will authorize with a single message to the server and a single response.
     */
    @Test
    public void testSingleMessageAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setUseHostNameAsBookieID(true);
        bookieConf.setBookieAuthProviderFactoryClass(
            SASLBookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
            SASLClientProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        connectAndWriteToBookie(clientConf, ledgerId); // should succeed

        assertFalse(ledgerId.get() == -1);
        assertEquals("Should have entry", 1, entryCount(ledgerId.get(), clientConf));
    }

    @Test
    public void testNotAllowedClientId() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setUseHostNameAsBookieID(true);
        bookieConf.setBookieAuthProviderFactoryClass(
            SASLBookieAuthProviderFactory.class.getName());
        bookieConf.setProperty(SaslConstants.JAAS_CLIENT_ALLOWED_IDS, "nobody");

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
            SASLClientProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId);
            fail("should not be able to access the bookie");
        } catch (BKUnauthorizedAccessException err) {
        }

    }

    BookieServer startAndStoreBookie(ServerConfiguration conf) throws Exception {
        System.setProperty(SaslConstants.SASL_SERVICE_NAME, non_default_sasl_service_name);
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
