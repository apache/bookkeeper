/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.tls;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.auth.AuthCallbacks;
import org.apache.bookkeeper.auth.AuthToken;
import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BookKeeperClientStats;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieConnectionPeer;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.ClientConnectionPeer;
import org.apache.bookkeeper.proto.TestPerChannelBookieClient;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.tls.TLSContextFactory.KeyStoreType;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests with TLS enabled.
 */
@RunWith(Parameterized.class)
public class TestTLS extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TestPerChannelBookieClient.class);

    private static boolean secureClientSideChannel = false;
    private static Collection<Object> secureClientSideChannelPrincipals = null;

    private static boolean secureBookieSideChannel = false;
    private static Collection<Object> secureBookieSideChannelPrincipals = null;

    private KeyStoreType clientKeyStoreFormat;
    private KeyStoreType clientTrustStoreFormat;
    private KeyStoreType serverKeyStoreFormat;
    private KeyStoreType serverTrustStoreFormat;
    private final boolean useV2Protocol;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                 { "JKS", "JKS", false },
                 { "PEM", "PEM", false },
                 { "PEM", "PEM", true },
                 { "PKCS12", "PKCS12", false },
                 { "JKS", "PEM", false },
                 { "PEM", "PKCS12", false },
                 { "PKCS12", "JKS", false }
            });
    }
    public TestTLS(String keyStoreFormat,
                   String trustStoreFormat,
                   boolean useV2Protocol) {
        super(3);
        this.clientKeyStoreFormat = KeyStoreType.valueOf(keyStoreFormat);
        this.clientTrustStoreFormat = KeyStoreType.valueOf(trustStoreFormat);
        this.serverKeyStoreFormat = KeyStoreType.valueOf(keyStoreFormat);
        this.serverTrustStoreFormat = KeyStoreType.valueOf(trustStoreFormat);
        this.useV2Protocol = useV2Protocol;
    }

    private String getResourcePath(String resource) throws Exception {
        return this.getClass().getClassLoader().getResource(resource).toURI().getPath();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        /* client configuration */
        baseClientConf.setTLSProviderFactoryClass(TLSContextFactory.class.getName());
        baseClientConf.setTLSClientAuthentication(true);
        baseClientConf.setUseV2WireProtocol(useV2Protocol);

        switch (clientKeyStoreFormat) {
        case PEM:
            baseClientConf.setTLSKeyStoreType("PEM");
            baseClientConf.setTLSKeyStore(getResourcePath("client-key.pem"));
            baseClientConf.setTLSCertificatePath(getResourcePath("client-cert.pem"));

            break;
        case JKS:
            baseClientConf.setTLSKeyStoreType("JKS");
            baseClientConf.setTLSKeyStore(getResourcePath("client-key.jks"));
            baseClientConf.setTLSKeyStorePasswordPath(getResourcePath("keyStoreClientPassword.txt"));

            break;
        case PKCS12:
            baseClientConf.setTLSKeyStoreType("PKCS12");
            baseClientConf.setTLSKeyStore(getResourcePath("client-key.p12"));
            baseClientConf.setTLSKeyStorePasswordPath(getResourcePath("keyStoreClientPassword.txt"));

            break;
        default:
            throw new Exception("Invalid client keystore format" + clientKeyStoreFormat);
        }

        switch (clientTrustStoreFormat) {
        case PEM:
            baseClientConf.setTLSTrustStoreType("PEM");
            baseClientConf.setTLSTrustStore(getResourcePath("server-cert.pem"));

            break;
        case JKS:
            baseClientConf.setTLSTrustStoreType("JKS");
            baseClientConf.setTLSTrustStore(getResourcePath("server-key.jks"));
            baseClientConf.setTLSTrustStorePasswordPath(getResourcePath("keyStoreServerPassword.txt"));

            break;
        case PKCS12:
            baseClientConf.setTLSTrustStoreType("PKCS12");
            baseClientConf.setTLSTrustStore(getResourcePath("server-key.p12"));
            baseClientConf.setTLSTrustStorePasswordPath(getResourcePath("keyStoreServerPassword.txt"));

            break;
        default:
            throw new Exception("Invalid client keystore format" + clientTrustStoreFormat);
        }

        /* server configuration */
        baseConf.setTLSProviderFactoryClass(TLSContextFactory.class.getName());
        baseConf.setTLSClientAuthentication(true);

        switch (serverKeyStoreFormat) {
        case PEM:
            baseConf.setTLSKeyStoreType("PEM");
            baseConf.setTLSKeyStore(getResourcePath("server-key.pem"));
            baseConf.setTLSCertificatePath(getResourcePath("server-cert.pem"));

            break;
        case JKS:
            baseConf.setTLSKeyStoreType("JKS");
            baseConf.setTLSKeyStore(getResourcePath("server-key.jks"));
            baseConf.setTLSKeyStorePasswordPath(getResourcePath("keyStoreServerPassword.txt"));

            break;
        case PKCS12:
            baseConf.setTLSKeyStoreType("PKCS12");
            baseConf.setTLSKeyStore(getResourcePath("server-key.p12"));
            baseConf.setTLSKeyStorePasswordPath(getResourcePath("keyStoreServerPassword.txt"));

            break;
        default:
            throw new Exception("Invalid server keystore format" + serverKeyStoreFormat);
        }

        switch (serverTrustStoreFormat) {
        case PEM:
            baseConf.setTLSTrustStoreType("PEM");
            baseConf.setTLSTrustStore(getResourcePath("client-cert.pem"));

            break;
        case JKS:
            baseConf.setTLSTrustStoreType("JKS");
            baseConf.setTLSTrustStore(getResourcePath("client-key.jks"));
            baseConf.setTLSTrustStorePasswordPath(getResourcePath("keyStoreClientPassword.txt"));

            break;

        case PKCS12:
            baseConf.setTLSTrustStoreType("PKCS12");
            baseConf.setTLSTrustStore(getResourcePath("client-key.p12"));
            baseConf.setTLSTrustStorePasswordPath(getResourcePath("keyStoreClientPassword.txt"));

            break;
        default:
            throw new Exception("Invalid server keystore format" + serverTrustStoreFormat);
        }

        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Verify that a server will not start if tls is enabled but no cert is specified.
     */
    @Test
    public void testStartTLSServerNoKeyStore() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration().setTLSKeyStore(null);

        try {
            bs.add(startBookie(bookieConf));
            fail("Shouldn't have been able to start");
        } catch (SecurityException se) {
            assertTrue(true);
        }
    }

    /**
     * Verify handshake failure with a bad cert.
     */
    @Test
    public void testKeyMismatchFailure() throws Exception {
        // Valid test case only for PEM format keys
        assumeTrue(serverKeyStoreFormat == KeyStoreType.PEM);

        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);

        // restart a bookie with bad cert
        int restartBookieIdx = 0;
        ServerConfiguration bookieConf = bsConfs.get(restartBookieIdx)
                .setTLSCertificatePath(getResourcePath("client-cert.pem"));
        killBookie(restartBookieIdx);
        LOG.info("Sleeping for 1s before restarting bookie with bad cert");
        Thread.sleep(1000);
        bs.add(startBookie(bookieConf));
        bsConfs.add(bookieConf);

        // Create ledger and write entries
        BookKeeper client = new BookKeeper(clientConf);
        byte[] passwd = "testPassword".getBytes();
        int numEntries = 2;
        byte[] testEntry = "testEntry".getBytes();

        try (LedgerHandle lh = client.createLedger(numBookies, numBookies, DigestType.CRC32, passwd)) {
            for (int i = 0; i <= numEntries; i++) {
                lh.addEntry(testEntry);
            }
            fail("Should have failed with not enough bookies to write");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // expected
        }
    }

    /**
     * Verify that a server will not start if ssl is enabled but the cert password is incorrect.
     */
    @Test
    public void testStartTLSServerBadPassword() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration().setTLSKeyStorePasswordPath("badpassword");
        try {
            bs.add(startBookie(bookieConf));
            fail("Shouldn't have been able to start");
        } catch (SecurityException se) {
            assertTrue(true);
        }
    }

    private LedgerMetadata testClient(BookKeeper client, int clusterSize) throws Exception {
        byte[] passwd = "testPassword".getBytes();
        int numEntries = 100;
        long lid;
        byte[] testEntry = "testEntry".getBytes();
        try (LedgerHandle lh = client.createLedger(clusterSize, clusterSize, DigestType.CRC32, passwd)) {
            for (int i = 0; i <= numEntries; i++) {
                lh.addEntry(testEntry);
            }
            lid = lh.getId();
        }
        try (LedgerHandle lh = client.openLedger(lid, DigestType.CRC32, passwd)) {
            Enumeration<LedgerEntry> entries = lh.readEntries(0, numEntries);
            while (entries.hasMoreElements()) {
                LedgerEntry e = entries.nextElement();
                assertTrue("Entry contents incorrect", Arrays.equals(e.getEntry(), testEntry));
            }
            BookKeeperAdmin admin = new BookKeeperAdmin(client);
            return admin.getLedgerMetadata(lh);
        }
    }

    private LedgerMetadata testClient(ClientConfiguration conf, int clusterSize) throws Exception {
        try (BookKeeper client = new BookKeeper(conf)) {
            return testClient(client, clusterSize);
        }
    }

    /**
     * Verify the basic use of TLS. TLS client, TLS servers.
     */
    @Test
    public void testConnectToTLSClusterTLSClient() throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        testClient(clientConf, numBookies);
    }

    /**
     * Verify the basic use of TLS. TLS client, TLS servers with LocalTransport.
     */
    @Test
    public void testConnectToLocalTLSClusterTLSClient() throws Exception {
        // skip test
        if (useV2Protocol) {
            return;
        }
        ServerConfiguration serverConf = new ServerConfiguration();
        for (ServerConfiguration conf : bsConfs) {
            conf.setDisableServerSocketBind(true);
            conf.setEnableLocalTransport(true);
        }
        restartBookies(serverConf);

        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        testClient(clientConf, numBookies);
    }

    /**
     * Verify Bookie refreshes certs at configured duration.
     */
    @Test
    public void testRefreshDurationForBookieCerts() throws Exception {
        assumeTrue(serverKeyStoreFormat == KeyStoreType.PEM);
        ServerConfiguration serverConf = new ServerConfiguration();
        String originalTlsKeyFilePath = bsConfs.get(0).getTLSKeyStore();
        String invalidServerKey = getResourcePath("client-key.pem");
        File originalTlsCertFile = new File(originalTlsKeyFilePath);
        File newTlsKeyFile = IOUtils.createTempFileAndDeleteOnExit(originalTlsKeyFilePath, "refresh");
        // clean up temp file even if test fails
        newTlsKeyFile.deleteOnExit();
        File invalidServerKeyFile = new File(invalidServerKey);
        // copy invalid cert to new temp file
        FileUtils.copyFile(invalidServerKeyFile, newTlsKeyFile);
        long refreshDurationInSec = 1;
        for (ServerConfiguration conf : bsConfs) {
            conf.setTLSCertFilesRefreshDurationSeconds(1);
            conf.setTLSKeyStore(newTlsKeyFile.getAbsolutePath());
        }
        restartBookies(serverConf);

        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        try {
            testClient(clientConf, numBookies);
            Assert.fail("Should have fail due to invalid cert");
        } catch (Exception e) {
            // Ok.
        }

        // Sleep so, cert file can be refreshed
        Thread.sleep(refreshDurationInSec * 1000 + 1000);

        // copy valid key-file at given new location
        FileUtils.copyFile(originalTlsCertFile, newTlsKeyFile);
        newTlsKeyFile.setLastModified(System.currentTimeMillis() + 1000);
        // client should be successfully able to add entries over tls
        testClient(clientConf, numBookies);
        newTlsKeyFile.delete();
    }

    /**
     * Verify Bookkeeper-client refreshes certs at configured duration.
     */
    @Test
    public void testRefreshDurationForBookkeeperClientCerts() throws Exception {
        assumeTrue(serverKeyStoreFormat == KeyStoreType.PEM);

        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        String originalTlsCertFilePath = baseClientConf.getTLSCertificatePath();
        String invalidClientCert = getResourcePath("server-cert.pem");
        File originalTlsCertFile = new File(originalTlsCertFilePath);
        File newTlsCertFile = IOUtils.createTempFileAndDeleteOnExit(originalTlsCertFilePath, "refresh");
        // clean up temp file even if test fails
        newTlsCertFile.deleteOnExit();
        File invalidClientCertFile = new File(invalidClientCert);
        // copy invalid cert to new temp file
        FileUtils.copyFile(invalidClientCertFile, newTlsCertFile);
        long refreshDurationInSec = 2;
        clientConf.setTLSCertFilesRefreshDurationSeconds(1);
        clientConf.setTLSCertificatePath(newTlsCertFile.getAbsolutePath());

        // create a bookkeeper-client
        try (BookKeeper client = new BookKeeper(clientConf)) {
            byte[] testEntry = "testEntry".getBytes();
            byte[] passwd = "testPassword".getBytes();
            int totalAddEntries = 1;
            CountDownLatch latch = new CountDownLatch(totalAddEntries);
            AtomicInteger result = new AtomicInteger(-1);
            LedgerHandle lh = client.createLedger(1, 1, DigestType.CRC32, passwd);

            for (int i = 0; i <= totalAddEntries; i++) {
                lh.asyncAddEntry(testEntry, (rc, lgh, entryId, ctx) -> {
                    result.set(rc);
                    latch.countDown();
                }, null);
            }
            latch.await(1, TimeUnit.SECONDS);
            Assert.assertNotEquals(result.get(), BKException.Code.OK);

            // Sleep so, cert file can be refreshed
            Thread.sleep(refreshDurationInSec * 1000 + 1000);

            // copy valid key-file at given new location
            FileUtils.copyFile(originalTlsCertFile, newTlsCertFile);
            newTlsCertFile.setLastModified(System.currentTimeMillis() + 1000);
            // client should be successfully able to add entries over tls
            CountDownLatch latchWithValidCert = new CountDownLatch(totalAddEntries);
            AtomicInteger validCertResult = new AtomicInteger(-1);
            lh = client.createLedger(1, 1, DigestType.CRC32, passwd);
            for (int i = 0; i <= totalAddEntries; i++) {
                lh.asyncAddEntry(testEntry, (rc, lgh, entryId, ctx) -> {
                    validCertResult.set(rc);
                    latchWithValidCert.countDown();
                }, null);
            }
            latchWithValidCert.await(1, TimeUnit.SECONDS);
            Assert.assertEquals(validCertResult.get(), BKException.Code.OK);
            newTlsCertFile.delete();
        }
    }

    /**
     * Multiple clients, some with TLS, and some without TLS.
     */
    @Test
    public void testConnectToTLSClusterMixedClient() throws Exception {
        ClientConfiguration confWithTLS = new ClientConfiguration(baseClientConf);
        testClient(confWithTLS, numBookies);

        ClientConfiguration confNoTLS = new ClientConfiguration(baseClientConf);
        confNoTLS.setTLSProviderFactoryClass(null);
        testClient(confNoTLS, numBookies);
    }

    /**
     * Verify the basic use of TLS. TLS client, TLS servers. No Mutual Authentication.
     */
    @Test
    public void testConnectToTLSClusterTLSClientWithTLSNoAuthentication() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration(baseConf);
        serverConf.setTLSClientAuthentication(false);
        restartBookies(serverConf);

        ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        testClient(conf, numBookies);
    }

    /**
     * Verify the basic use of TLS. TLS client, TLS servers with mutual Auth.
     */
    @Test
    public void testConnectToTLSClusterTLSClientWithAuthentication() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        try {
            testClient(conf, numBookies);
        } catch (BKException.BKNotEnoughBookiesException nnbe) {
            fail("Client should be able to connect to bookie");
        }
    }

    /**
     * Verify that a client without tls enabled can connect to a cluster with TLS.
     */
    @Test
    public void testConnectToTLSClusterNonTLSClient() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        conf.setTLSProviderFactoryClass(null);
        try {
            testClient(conf, numBookies);
        } catch (BKException.BKNotEnoughBookiesException nnbe) {
            fail("non tls client should be able to connect to tls enabled bookies");
        }
    }

    /**
     * Verify that a client will fail to connect to a server if it has asked for TLS, but it is not available.
     */
    @Test
    public void testClientWantsTLSNoServersHaveIt() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration();
        for (ServerConfiguration conf : bsConfs) {
            conf.setTLSProviderFactoryClass(null);
        }
        restartBookies(serverConf);

        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        try {
            testClient(clientConf, numBookies);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKNotEnoughBookiesException nnbe) {
            // correct response
        }
    }

    /**
     * Verify that a client will be able to connect to a bookie cluster if it has asked for TLS, and there are enough
     * bookies with TLS enabled in the cluster, although few bookies do not have TLS enabled.
     */
    @Test
    public void testTLSClientButOnlyFewTLSServers() throws Exception {
        // disable TLS on initial set of bookies
        ServerConfiguration serverConf = new ServerConfiguration();
        for (ServerConfiguration conf : bsConfs) {
            conf.setTLSProviderFactoryClass(null);
        }
        restartBookies(serverConf);

        // add two bookies which support TLS
        baseConf.setTLSProviderFactoryClass(TLSContextFactory.class.getName());

        Set<Integer> tlsBookiePorts = new HashSet<>();
        tlsBookiePorts.add(startNewBookie());
        tlsBookiePorts.add(startNewBookie());

        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        LedgerMetadata metadata = testClient(clientConf, 2);
        assertTrue(metadata.getAllEnsembles().size() > 0);
        Collection<? extends List<BookieSocketAddress>> ensembles = metadata.getAllEnsembles().values();
        for (List<BookieSocketAddress> bookies : ensembles) {
            for (BookieSocketAddress bookieAddress : bookies) {
                int port = bookieAddress.getPort();
                assertTrue(tlsBookiePorts.contains(port));
            }
        }
    }

    /**
     * Verify that a client-side Auth plugin can access server certificates.
     */
    @Test
    public void testClientAuthPlugin() throws Exception {
        secureClientSideChannel = false;
        secureClientSideChannelPrincipals = null;
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);

        clientConf.setClientAuthProviderFactoryClass(AllowOnlyBookiesWithX509Certificates.class.getName());

        testClient(clientConf, numBookies);
        assertTrue(secureClientSideChannel);
        assertNotNull(secureClientSideChannelPrincipals);
        assertTrue(!secureClientSideChannelPrincipals.isEmpty());
        assertTrue(secureClientSideChannelPrincipals.iterator().next() instanceof Certificate);
        Certificate cert = (Certificate) secureClientSideChannelPrincipals.iterator().next();
        assertTrue(cert instanceof X509Certificate);
    }

    /**
     * Verify that a bookie-side Auth plugin can access server certificates.
     */
    @Test
    public void testBookieAuthPluginRequireClientTLSAuthentication() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration(baseConf);
        serverConf.setBookieAuthProviderFactoryClass(AllowOnlyClientsWithX509Certificates.class.getName());
        restartBookies(serverConf);

        secureBookieSideChannel = false;
        secureBookieSideChannelPrincipals = null;
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);

        testClient(clientConf, numBookies);
        assertTrue(secureBookieSideChannel);
        assertNotNull(secureBookieSideChannelPrincipals);
        assertTrue(!secureBookieSideChannelPrincipals.isEmpty());
        assertTrue(secureBookieSideChannelPrincipals.iterator().next() instanceof Certificate);
        Certificate cert = (Certificate) secureBookieSideChannelPrincipals.iterator().next();
        assertTrue(cert instanceof X509Certificate);
    }

    /**
     * Verify that a bookie-side Auth plugin can access server certificates over LocalTransport.
     */
    @Test
    public void testBookieAuthPluginRequireClientTLSAuthenticationLocal() throws Exception {
        if (useV2Protocol) {
            return;
        }

        ServerConfiguration serverConf = new ServerConfiguration(baseConf);
        serverConf.setBookieAuthProviderFactoryClass(AllowOnlyClientsWithX509Certificates.class.getName());
        serverConf.setDisableServerSocketBind(true);
        serverConf.setEnableLocalTransport(true);
        restartBookies(serverConf);

        secureBookieSideChannel = false;
        secureBookieSideChannelPrincipals = null;
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);

        testClient(clientConf, numBookies);
        assertTrue(secureBookieSideChannel);
        assertNotNull(secureBookieSideChannelPrincipals);
        assertTrue(!secureBookieSideChannelPrincipals.isEmpty());
        assertTrue(secureBookieSideChannelPrincipals.iterator().next() instanceof Certificate);
        Certificate cert = (Certificate) secureBookieSideChannelPrincipals.iterator().next();
        assertTrue(cert instanceof X509Certificate);
    }

    /**
     * Verify that a bookie-side Auth plugin can access server certificates.
     */
    @Test
    public void testBookieAuthPluginDenyAccesstoClientWithoutTLSAuthentication() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration(baseConf);
        serverConf.setTLSClientAuthentication(false);
        serverConf.setBookieAuthProviderFactoryClass(AllowOnlyClientsWithX509Certificates.class.getName());
        restartBookies(serverConf);

        secureBookieSideChannel = false;
        secureBookieSideChannelPrincipals = null;
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        clientConf.setTLSClientAuthentication(false);

        try {
            testClient(clientConf, numBookies);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKUnauthorizedAccessException authFailed) {
        }

        assertTrue(secureBookieSideChannel);
        assertNotNull(secureBookieSideChannelPrincipals);
        assertTrue(secureBookieSideChannelPrincipals.isEmpty());
    }

    /**
     * Verify that a bookie-side Auth plugin can access server certificates over LocalTransport.
     */
    @Test
    public void testBookieAuthPluginDenyAccessToClientWithoutTLSAuthenticationLocal() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration(baseConf);
        serverConf.setTLSClientAuthentication(false);
        serverConf.setBookieAuthProviderFactoryClass(AllowOnlyClientsWithX509Certificates.class.getName());
        serverConf.setDisableServerSocketBind(true);
        serverConf.setEnableLocalTransport(true);
        restartBookies(serverConf);

        secureBookieSideChannel = false;
        secureBookieSideChannelPrincipals = null;
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        clientConf.setTLSClientAuthentication(false);

        try {
            testClient(clientConf, numBookies);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKUnauthorizedAccessException authFailed) {
        }

        assertTrue(secureBookieSideChannel);
        assertNotNull(secureBookieSideChannelPrincipals);
        assertTrue(secureBookieSideChannelPrincipals.isEmpty());
    }

    /**
     * Verify that a bookie-side Auth plugin can access server certificates.
     */
    @Test
    public void testBookieAuthPluginDenyAccessToClientWithoutTLS() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration(baseConf);
        serverConf.setBookieAuthProviderFactoryClass(AllowOnlyClientsWithX509Certificates.class.getName());
        restartBookies(serverConf);

        secureBookieSideChannel = false;
        secureBookieSideChannelPrincipals = null;
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        clientConf.setTLSProviderFactoryClass(null);

        try {
            testClient(clientConf, numBookies);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKUnauthorizedAccessException authFailed) {
        }

        assertFalse(secureBookieSideChannel);
        assertNull(secureBookieSideChannelPrincipals);
    }

    private static class AllowOnlyBookiesWithX509Certificates implements ClientAuthProvider.Factory {

        @Override
        public String getPluginName() {
            return "tls";
        }

        @Override
        public void init(ClientConfiguration conf) {
        }

        @Override
        public ClientAuthProvider newProvider(ClientConnectionPeer addr,
                final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new ClientAuthProvider() {

                AuthCallbacks.GenericCallback<AuthToken> completeCallback;

                @Override
                public void init(AuthCallbacks.GenericCallback<AuthToken> cb) {
                    this.completeCallback = cb;
                }

                @Override
                public void onProtocolUpgrade() {
                    secureClientSideChannel = addr.isSecure();
                    secureClientSideChannelPrincipals = addr.getProtocolPrincipals();
                    Collection<Object> certificates = addr.getProtocolPrincipals();
                    if (addr.isSecure() && !certificates.isEmpty()) {
                        assertTrue(certificates.iterator().next() instanceof X509Certificate);
                        completeCallback.operationComplete(BKException.Code.OK, AuthToken.NULL);
                    } else {
                        completeCallback.operationComplete(BKException.Code.UnauthorizedAccessException,
                                AuthToken.NULL);
                    }
                }

                @Override
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                }
            };
        }
    }

    private static class AllowOnlyClientsWithX509Certificates implements BookieAuthProvider.Factory {

        @Override
        public String getPluginName() {
            return "tls";
        }

        @Override
        public void init(ServerConfiguration conf) throws IOException {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {

                AuthCallbacks.GenericCallback<Void> completeCallback = completeCb;

                @Override
                public void onProtocolUpgrade() {
                    secureBookieSideChannel = addr.isSecure();
                    secureBookieSideChannelPrincipals = addr.getProtocolPrincipals();
                    Collection<Object> certificates = addr.getProtocolPrincipals();
                    if (addr.isSecure() && !certificates.isEmpty()) {
                        assertTrue(certificates.iterator().next() instanceof X509Certificate);
                        completeCallback.operationComplete(BKException.Code.OK, null);
                    } else {
                        completeCallback.operationComplete(BKException.Code.UnauthorizedAccessException, null);
                    }
                }

                @Override
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                }
            };
        }
    }

    /**
     * Verify that a client will fail to connect to a server if it has asked for TLS, but it is not available. Verify
     * that if there are enough TLS servers to fill the ensemble, it will eventually use those rather than the non-TLS
     */
    @Test
    public void testMixedCluster() throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        int origNumBookies = numBookies;

        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setTLSProviderFactoryClass(TLSContextFactory.class.getName());
        bs.add(startBookie(bookieConf));
        testClient(clientConf, origNumBookies + 1);
    }

    /**
     * Verify that if the server hangs while an TLS client is trying to connect, the client can continue.
     */
    @Test
    public void testHungServer() throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        CountDownLatch latch = new CountDownLatch(1);
        sleepBookie(getBookie(0), latch);
        try {
            testClient(clientConf, numBookies);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKNotEnoughBookiesException nnbe) {
            // correct response
        }
        LOG.info("latch countdown");
        latch.countDown();
    }

    /**
     * Verify TLS and non-TLS channel counters.
     */
    @Test
    public void testTLSChannelCounters() throws Exception {
        ClientConfiguration tlsClientconf = new ClientConfiguration(baseClientConf)
                .setNumChannelsPerBookie(1);
        ClientConfiguration nonTlsClientconf = new ClientConfiguration(baseClientConf)
                .setNumChannelsPerBookie(1)
                .setTLSProviderFactoryClass(null);

        TestStatsProvider tlsStatsProvider = new TestStatsProvider();
        TestStatsProvider nonTlsStatsProvider = new TestStatsProvider();
        BookKeeperTestClient tlsClient = new BookKeeperTestClient(tlsClientconf, tlsStatsProvider);
        BookKeeperTestClient nonTlsClient = new BookKeeperTestClient(nonTlsClientconf, nonTlsStatsProvider);

        // IO load from clients
        testClient(tlsClient, numBookies);
        testClient(nonTlsClient, numBookies);

        // verify stats
        for (int i = 0; i < numBookies; i++) {
            BookieServer bookie = bs.get(i);
            InetSocketAddress addr = bookie.getLocalAddress().getSocketAddress();
            StringBuilder nameBuilder = new StringBuilder(BookKeeperClientStats.CHANNEL_SCOPE)
                    .append(".")
                    .append(addr.getAddress().getHostAddress()
                    .replace('.', '_')
                    .replace('-', '_'))
                    .append("_")
                    .append(addr.getPort())
                    .append(".");

            // check stats on TLS enabled client
            assertEquals("Mismatch TLS channel count", 1,
                    tlsClient.getTestStatsProvider().getCounter(nameBuilder.toString()
                    + BookKeeperClientStats.ACTIVE_TLS_CHANNEL_COUNTER).get().longValue());
            assertEquals("TLS handshake failure unexpected", 0,
                    tlsClient.getTestStatsProvider().getCounter(nameBuilder.toString()
                    + BookKeeperClientStats.FAILED_TLS_HANDSHAKE_COUNTER).get().longValue());
            assertEquals("Mismatch non-TLS channel count", 0,
                    tlsClient.getTestStatsProvider().getCounter(nameBuilder.toString()
                    + BookKeeperClientStats.ACTIVE_NON_TLS_CHANNEL_COUNTER).get().longValue());
            assertEquals("Connection failures unexpected", 0,
                    tlsClient.getTestStatsProvider().getCounter(nameBuilder.toString()
                    + BookKeeperClientStats.FAILED_CONNECTION_COUNTER).get().longValue());

            // check stats on non-TLS enabled client
            assertEquals("Mismatch TLS channel count", 0,
                    nonTlsClient.getTestStatsProvider().getCounter(nameBuilder.toString()
                    + BookKeeperClientStats.ACTIVE_TLS_CHANNEL_COUNTER).get().longValue());
            assertEquals("TLS handshake failure unexpected", 0,
                    nonTlsClient.getTestStatsProvider().getCounter(nameBuilder.toString()
                    + BookKeeperClientStats.FAILED_TLS_HANDSHAKE_COUNTER).get().longValue());
            assertEquals("Mismatch non-TLS channel count", 1,
                    nonTlsClient.getTestStatsProvider().getCounter(nameBuilder.toString()
                    + BookKeeperClientStats.ACTIVE_NON_TLS_CHANNEL_COUNTER).get().longValue());
            assertEquals("Connection failures unexpected", 0,
                    nonTlsClient.getTestStatsProvider().getCounter(nameBuilder.toString()
                    + BookKeeperClientStats.FAILED_CONNECTION_COUNTER).get().longValue());

            bookie.shutdown();
            assertEquals("Mismatch TLS channel count", 0,
                    tlsClient.getTestStatsProvider().getCounter(nameBuilder.toString()
                    + BookKeeperClientStats.ACTIVE_TLS_CHANNEL_COUNTER).get().longValue());
            assertEquals("Mismatch non-TLS channel count", 0,
                    nonTlsClient.getTestStatsProvider().getCounter(nameBuilder.toString()
                    + BookKeeperClientStats.ACTIVE_NON_TLS_CHANNEL_COUNTER).get().longValue());

        }
    }

    /**
     * Verify handshake failure due to missing entry in trust store.
     */
    @Test
    public void testHandshakeFailure() throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf)
                .setNumChannelsPerBookie(1);

        // restart a bookie with wrong trust store
        int restartBookieIdx = 0;
        ServerConfiguration badBookieConf = bsConfs.get(restartBookieIdx);

        switch (serverTrustStoreFormat) {
            case PEM:
                badBookieConf.setTLSTrustStore(getResourcePath("server-cert.pem"));
                break;
            case JKS:
                badBookieConf.setTLSTrustStore(getResourcePath("server-key.jks"))
                        .setTLSTrustStorePasswordPath(getResourcePath("keyStoreServerPassword.txt"));
                break;
            case PKCS12:
                badBookieConf.setTLSTrustStore(getResourcePath("server-key.p12"))
                        .setTLSTrustStorePasswordPath(getResourcePath("keyStoreServerPassword.txt"));
                break;
            default:
                throw new Exception("Unrecognized trust store format: " + serverTrustStoreFormat);
        }


        killBookie(restartBookieIdx);
        LOG.info("Sleeping for 1s before restarting bookie with bad cert");
        Thread.sleep(1000);
        BookieServer bookie = startBookie(badBookieConf);
        bs.add(bookie);
        bsConfs.add(badBookieConf);

        // Create ledger and write entries
        TestStatsProvider testStatsProvider = new TestStatsProvider();
        BookKeeperTestClient client = new BookKeeperTestClient(clientConf, testStatsProvider);
        byte[] passwd = "testPassword".getBytes();
        int numEntries = 2;
        byte[] testEntry = "testEntry".getBytes();

        // should fail to write entries whey WQ == AQ == 3
        try (LedgerHandle lh = client.createLedger(numBookies, numBookies, numBookies, DigestType.CRC32, passwd)) {
            for (int i = 0; i <= numEntries; i++) {
                lh.addEntry(testEntry);
            }
            fail("Should have failed with not enough bookies to write");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // expected
        }

        // check failed handshake counter
        InetSocketAddress addr = bookie.getLocalAddress().getSocketAddress();
        StringBuilder nameBuilder = new StringBuilder(BookKeeperClientStats.CHANNEL_SCOPE)
                .append(".")
                .append(addr.getAddress().getHostAddress()
                        .replace('.', '_')
                        .replace('-', '_'))
                .append("_")
                .append(addr.getPort())
                .append(".");

        assertEquals("TLS handshake failure expected", 1,
                client.getTestStatsProvider().getCounter(nameBuilder.toString()
                + BookKeeperClientStats.FAILED_TLS_HANDSHAKE_COUNTER).get().longValue());
    }
}
