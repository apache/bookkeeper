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
package org.apache.bookkeeper.proto.ssl;

import org.junit.*;
import java.util.concurrent.CountDownLatch;
import java.util.Enumeration;
import java.util.Arrays;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;

import org.apache.commons.io.IOUtils;
import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.FileOutputStream;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.bookkeeper.auth.AuthCallbacks;
import org.apache.bookkeeper.auth.AuthToken;
import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.bookie.BookieConnectionPeer;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.ClientConnectionPeer;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.TestPerChannelBookieClient;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests with SSL enabled.
 */
public class TestSSL extends BookKeeperClusterTestCase {

    static Logger LOG = LoggerFactory.getLogger(TestPerChannelBookieClient.class);
    private final static String CERT_RESOURCE = "/localhost.p12";
    private final static String CERT_PASSWORD = "apache";

    private final static String CLIENT_CERT_RESOURCE = "/client.p12";
    private final static String CLIENT_CERT_PASSWORD = "apache";

    private static boolean secureClientSideChannel = false;
    private static Collection<Object> secureClientSideChannelPrincipals = null;

    private static boolean secureBookieSideChannel = false;
    private static Collection<Object> secureBookieSideChannelPrincipals = null;


    public TestSSL() {
        super(3);
        baseConf.setSSLEnabled(true).setSSLKeyStore(CERT_RESOURCE)
            .setSSLKeyStorePassword(CERT_PASSWORD);
    }

    /**
     * Verify that a server will not start if ssl is enabled but no cert is specified
     */
    @Test(timeout = 60000)
    public void testStartSSLServerNoKeyStore() throws Exception {
        ServerConfiguration conf = newServerConfiguration()
            .setSSLEnabled(true).setSSLKeyStore("");
        try {
            startBookie(conf);
            fail("Shouldn't have been able to start");
        } catch (IOException ioe) {
            assertTrue(true);
        }
    }

    /**
     * Verify that a server will not start if ssl is enabled but the cert password is incorrect
     */
    @Test(timeout = 60000)
    public void testStartSSLServerBadPassword() throws Exception {
        ServerConfiguration conf = newServerConfiguration()
            .setSSLEnabled(true).setSSLKeyStorePassword("badpassword");
        try {
            startBookie(conf);
            fail("Shouldn't have been able to start");
        } catch (IOException ioe) {
            assertTrue(true);
        }
    }

    /**
     * Verify that a server can start while loading the cert from a file rather than a resource.
     */
    @Test(timeout = 60000)
    public void testStartSSLServerFileCert() throws Exception {
        File f = File.createTempFile("keystore", ".p12");
        try {
            try (InputStream in = getClass().getResourceAsStream(CERT_RESOURCE)) {
                try (FileOutputStream out = new FileOutputStream(f);) {
                    IOUtils.copy(in, out);
                }
            }
            ServerConfiguration conf = newServerConfiguration()
                .setSSLEnabled(true)
                .setSSLKeyStore(f.toString())
                .setSSLKeyStorePassword(CERT_PASSWORD);
            startBookie(conf);
        } finally {
            f.delete();
        }
    }

    private LedgerMetadata testClient(ClientConfiguration conf, int clusterSize) throws Exception {
        try (BookKeeper client = new BookKeeper(conf);) {
            byte[] passwd = "testPassword".getBytes();
            int numEntries = 100;
            long lid;
            byte[] testEntry = "testEntry".getBytes();
            try (LedgerHandle lh = client.createLedger(clusterSize, clusterSize,
                DigestType.CRC32, passwd);) {
                for (int i = 0; i <= numEntries; i++) {
                    lh.addEntry(testEntry);
                }
                lid = lh.getId();
            }
            try (LedgerHandle lh = client.openLedger(lid, DigestType.CRC32, passwd);) {
                Enumeration<LedgerEntry> entries = lh.readEntries(0, numEntries);
                while (entries.hasMoreElements()) {
                    LedgerEntry e = entries.nextElement();
                    assertTrue("Entry contents incorrect", Arrays.equals(e.getEntry(), testEntry));
                }
                BookKeeperAdmin admin = new BookKeeperAdmin(client);
                return admin.getLedgerMetadata(lh);
            }
        }
    }

    /**
     * Verify the basic use of SSL. SSL client, SSL servers
     */
    @Test(timeout = 60000)
    public void testConnectToSSLClusterSSLClient() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf).setUseSSL(true);
        testClient(conf, numBookies);
    }


    /**
     * Multiple clients, some with SSL, and some without SSL
     */
    @Test(timeout = 60000)
    public void testConnectToSSLClusterMixedClient() throws Exception {
        ClientConfiguration confWithSSL = new ClientConfiguration(baseClientConf).setUseSSL(true);
        testClient(confWithSSL, numBookies);
        
        ClientConfiguration confNoSSL = new ClientConfiguration(baseClientConf).setUseSSL(false);
        testClient(confNoSSL, numBookies);
    }

    /**
     * Verify the basic use of SSL. SSL client, SSL servers
     */
    @Test(timeout = 60000)
    public void testConnectToSSLClusterSSLClientWithSSLAuth() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf)
            .setUseSSL(true)
            .setClientSSLAuthentication(true)
            .setClientSSLKeyStore(CLIENT_CERT_RESOURCE)
            .setClientSSLKeyStorePassword(CLIENT_CERT_PASSWORD);
        testClient(conf, numBookies);
    }

    /**
     * Verify that a client without ssl enabled can connect to a cluster with SSL
     */
    @Test(timeout = 60000)
    public void testConnectToSSLClusterNonSSLClient() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf).setUseSSL(false);
        testClient(conf, numBookies);
    }

    /**
     * Verify that a client will fail to connect to a server if it has asked for SSL, but it is not available.
     */
    @Test(timeout = 60000)
    public void testClientWantsSSLNoServersHaveIt() throws Exception {
        baseConf.setSSLEnabled(false);
        restartBookies();

        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf).setUseSSL(true);
        try {
            testClient(clientConf, numBookies);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKNotEnoughBookiesException nnbe) {
            // correct response
        }
    }

    /**
     * Verify that a client will fail to connect to a server if it has asked for SSL, but it is not available.
     */
    @Test(timeout = 60000)
    public void testClientWantsButOnlySSLSomeServerHaveIt() throws Exception {
        // initial bookies will not support SSL
        baseConf.setSSLEnabled(false);
        restartBookies();

        // add two bookies which support SSL
        baseConf.setSSLEnabled(true);

        Set<Integer> sslBookiePorts = new HashSet<>();
        sslBookiePorts.add(startNewBookie());
        sslBookiePorts.add(startNewBookie());

        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf).setUseSSL(true);
        LedgerMetadata metadata = testClient(clientConf, 2);
        assertTrue(metadata.getEnsembles().size()>0);
        Collection<ArrayList<BookieSocketAddress>> ensambles = metadata.getEnsembles().values();
        for (ArrayList<BookieSocketAddress> bookies : ensambles) {
            for (BookieSocketAddress bookieAddress :bookies) {
                int port = bookieAddress.getPort();
                assertTrue(sslBookiePorts.contains(port));
            }
        }
    }

    /**
     * Verify that a client will not connect to a self-signed certificate
     */
    @Test(timeout = 60000)
    public void testClientVerifiesSSLCertificates() throws Exception {
         ClientConfiguration clientConf = new ClientConfiguration(baseClientConf)
             .setUseSSL(true)
             .setVerifySSLCertificates(true);
        try {
            testClient(clientConf, numBookies);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKNotEnoughBookiesException nnbe) {
            // correct response
        }
    }

    /**
     * Verify that a client-side Auth plugin can access server certificates
     */
    @Test(timeout = 60000)
    public void testClientAuthPlugin() throws Exception {
        secureClientSideChannel = false;
        secureClientSideChannelPrincipals = null;
         ClientConfiguration clientConf = new ClientConfiguration(baseClientConf)
             .setUseSSL(true);

        clientConf.setClientAuthProviderFactoryClass(
                AllowOnlyBookiesWithX509Certificates.class.getName());
        
        testClient(clientConf, numBookies);
        assertTrue(secureClientSideChannel);
        assertNotNull(secureClientSideChannelPrincipals);
        assertTrue(!secureClientSideChannelPrincipals.isEmpty());
        assertTrue(secureClientSideChannelPrincipals.iterator().next() instanceof Certificate);
        Certificate cert = (Certificate) secureClientSideChannelPrincipals.iterator().next();
        assertTrue(cert instanceof X509Certificate);
    }

    /**
     * Verify that a bookie-side Auth plugin can access server certificates
     */
    @Test(timeout = 60000)
    public void testBookieAuthPluginRequireClientSSLAuthentication() throws Exception {
        baseConf.setBookieAuthProviderFactoryClass(
                AllowOnlyClientsWithX509Certificates.class.getName());
        restartBookies();

        secureBookieSideChannel = false;
        secureBookieSideChannelPrincipals = null;
         ClientConfiguration clientConf = new ClientConfiguration(baseClientConf)
             .setUseSSL(true)
            .setClientSSLAuthentication(true)
            .setClientSSLKeyStore(CLIENT_CERT_RESOURCE)
            .setClientSSLKeyStorePassword(CLIENT_CERT_PASSWORD);

        testClient(clientConf, numBookies);
        assertTrue(secureBookieSideChannel);
        assertNotNull(secureBookieSideChannelPrincipals);
        assertTrue(!secureBookieSideChannelPrincipals.isEmpty());
        assertTrue(secureBookieSideChannelPrincipals.iterator().next() instanceof Certificate);
        Certificate cert = (Certificate) secureBookieSideChannelPrincipals.iterator().next();
        assertTrue(cert instanceof X509Certificate);
    }

    /**
     * Verify that a bookie-side Auth plugin can access server certificates
     */
    @Test(timeout = 60000)
    public void testBookieAuthPluginDenyAccesstoClientWithoutSSLAuthentication() throws Exception {
        baseConf.setBookieAuthProviderFactoryClass(
                AllowOnlyClientsWithX509Certificates.class.getName());
        restartBookies();

        secureBookieSideChannel = false;
        secureBookieSideChannelPrincipals = null;
         ClientConfiguration clientConf = new ClientConfiguration(baseClientConf)
             .setUseSSL(true);

        try {
            testClient(clientConf, numBookies);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKUnauthorizedAccessException authFailed){
        }

        assertTrue(secureBookieSideChannel);
        assertNotNull(secureBookieSideChannelPrincipals);
        assertTrue(secureBookieSideChannelPrincipals.isEmpty());
    }

    /**
     * Verify that a bookie-side Auth plugin can access server certificates
     */
    @Test(timeout = 60000)
    public void testBookieAuthPluginDenyAccessToClientWithoutSSL() throws Exception {
        baseConf.setBookieAuthProviderFactoryClass(
                AllowOnlyClientsWithX509Certificates.class.getName());
        restartBookies();

        secureBookieSideChannel = false;
        secureBookieSideChannelPrincipals = null;
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);

        try {
            testClient(clientConf, numBookies);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKUnauthorizedAccessException authFailed){
        }

        assertFalse(secureBookieSideChannel);
        assertNull(secureBookieSideChannelPrincipals);
    }

    private static class AllowOnlyBookiesWithX509Certificates
        implements ClientAuthProvider.Factory {

        @Override
        public String getPluginName() {
            return "ssl";
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
                        completeCallback.operationComplete(BKException.Code.UnauthorizedAccessException, AuthToken.NULL);
                    }
                }

                @Override
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                }
            };
        }
    }

    private static class AllowOnlyClientsWithX509Certificates
        implements BookieAuthProvider.Factory {

        @Override
        public String getPluginName() {
            return "ssl";
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
     * Verify that a client will fail to connect to a server if it has asked for SSL, but it is not available. Verify
     * that if there are enough SSL servers to fill the ensemble, it will eventually use those rather than the non-SSL
     */
    @Test(timeout = 60000)
    public void testMixedCluster() throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf).setUseSSL(true);
        int origNumBookies = numBookies;

        ServerConfiguration bookieConf = newServerConfiguration()
            .setSSLEnabled(false);
        startBookie(bookieConf);
        try {
            testClient(clientConf, origNumBookies + 1);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKNotEnoughBookiesException nnbe) {
            // correct response
        }

        bookieConf = newServerConfiguration()
            .setSSLEnabled(true);
        startBookie(bookieConf);
        testClient(clientConf, origNumBookies + 1);
    }

    /**
     * Verify that if the server hangs while an SSL client is trying to connect, the client can continue.
     */
    @Test(timeout = 60000)
    public void testHungServer() throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf).setUseSSL(true);
        CountDownLatch latch = new CountDownLatch(1);
        sleepBookie(getBookie(0), latch);
        try {
            testClient(clientConf, numBookies);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKNotEnoughBookiesException nnbe) {
            // correct response
        }
        latch.countDown();
    }
}
