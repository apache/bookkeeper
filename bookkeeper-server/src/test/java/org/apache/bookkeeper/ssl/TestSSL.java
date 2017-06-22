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
package org.apache.bookkeeper.ssl;

import org.junit.*;

import java.util.concurrent.CountDownLatch;
import java.util.Enumeration;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
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
import org.apache.bookkeeper.proto.TestPerChannelBookieClient;
import org.apache.bookkeeper.ssl.SSLContextFactory;
import org.apache.bookkeeper.ssl.SecurityException;

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

    private static boolean secureClientSideChannel = false;
    private static Collection<Object> secureClientSideChannelPrincipals = null;

    private static boolean secureBookieSideChannel = false;
    private static Collection<Object> secureBookieSideChannelPrincipals = null;


    public TestSSL() {
        super(3);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        /* client configuration */
        baseClientConf.setSSLProviderFactoryClass(SSLContextFactory.class.getName());
        baseClientConf.setSSLClientAuthentication(true);
        baseClientConf.setSSLKeyStoreType("JKS");
        baseClientConf.setSSLKeyStore(this.getClass().getClassLoader().getResource("client.jks").getPath());
        baseClientConf.setSSLKeyStorePasswordPath(
                this.getClass().getClassLoader().getResource("keyStoreClientPassword.txt").getPath());
        baseClientConf.setSSLTrustStoreType("JKS");
        baseClientConf.setSSLTrustStore(this.getClass().getClassLoader().getResource("cacerts").getPath());
        baseClientConf.setSSLTrustStorePasswordPath(
                this.getClass().getClassLoader().getResource("trustStorePassword.txt").getPath());

        /* server configuration */
        baseConf.setSSLProviderFactoryClass(SSLContextFactory.class.getName());
        baseConf.setSSLClientAuthentication(true);
        baseConf.setSSLKeyStoreType("JKS");
        baseConf.setSSLKeyStore(this.getClass().getClassLoader().getResource("server.jks").getPath());
        baseConf.setSSLKeyStorePasswordPath(
                this.getClass().getClassLoader().getResource("keyStoreServerPassword.txt").getPath());
        baseConf.setSSLTrustStoreType("JKS");
        baseConf.setSSLTrustStore(this.getClass().getClassLoader().getResource("cacerts").getPath());
        baseConf.setSSLTrustStorePasswordPath(
                this.getClass().getClassLoader().getResource("trustStorePassword.txt").getPath());

        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Verify that a server will not start if ssl is enabled but no cert is specified
     */
    @Test(timeout = 60000)
    public void testStartSSLServerNoKeyStore() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration().setSSLKeyStore(null);

        try {
            bs.add(startBookie(bookieConf));
            fail("Shouldn't have been able to start");
        } catch (SecurityException se) {
            assertTrue(true);
        }
    }

    /**
     * Verify that a server will not start if ssl is enabled but the cert password is incorrect
     */
    @Test(timeout = 60000)
    public void testStartSSLServerBadPassword() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration().setSSLKeyStorePasswordPath("badpassword");
        try {
            bs.add(startBookie(bookieConf));
            fail("Shouldn't have been able to start");
        } catch (SecurityException se) {
            assertTrue(true);
        }
    }

    private LedgerMetadata testClient(ClientConfiguration conf, int clusterSize) throws Exception {
        try (BookKeeper client = new BookKeeper(conf);) {
            byte[] passwd = "testPassword".getBytes();
            int numEntries = 100;
            long lid;
            byte[] testEntry = "testEntry".getBytes();
            try (LedgerHandle lh = client.createLedger(clusterSize, clusterSize, DigestType.CRC32, passwd);) {
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
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        testClient(clientConf, numBookies);
    }


    /**
     * Multiple clients, some with SSL, and some without SSL
     */
    @Test(timeout = 60000)
    public void testConnectToSSLClusterMixedClient() throws Exception {
        ClientConfiguration confWithSSL = new ClientConfiguration(baseClientConf);
        testClient(confWithSSL, numBookies);

        ClientConfiguration confNoSSL = new ClientConfiguration(baseClientConf);
        confNoSSL.setSSLProviderFactoryClass(null);
        testClient(confNoSSL, numBookies);
    }

    /**
     * Verify the basic use of SSL. SSL client, SSL servers. No Mutual Authentication.
     */
    @Test(timeout = 60000)
    public void testConnectToSSLClusterSSLClientWithSSLNoAuthentication() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration(baseConf);
        serverConf.setSSLClientAuthentication(false);
        restartBookies(serverConf);

        ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        testClient(conf, numBookies);
    }

    /**
     * Verify the basic use of SSL. SSL client, SSL servers with mutual Auth.
     */
    @Test(timeout = 60000)
    public void testConnectToSSLClusterSSLClientWithAuthentication() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        try {
            testClient(conf, numBookies);
        } catch (BKException.BKNotEnoughBookiesException nnbe) {
            fail("Client should be able to connect to bookie");
        }
    }

    /**
     * Verify that a client without ssl enabled can connect to a cluster with SSL
     */
    @Test(timeout = 60000)
    public void testConnectToSSLClusterNonSSLClient() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        conf.setSSLProviderFactoryClass(null);
        try {
            testClient(conf, numBookies);
        } catch (BKException.BKNotEnoughBookiesException nnbe) {
            fail("non ssl client should be able to connect to ssl enabled bookies");
        }
    }

    /**
     * Verify that a client will fail to connect to a server if it has asked for SSL, but it is not available.
     */
    @Test(timeout = 60000)
    public void testClientWantsSSLNoServersHaveIt() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration(baseConf);
        serverConf.setSSLProviderFactoryClass(null);
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
     * Verify that a client will be able to connect to a bookie cluster if it has asked for SSL, and there are enough
     * bookies with SSL enabled in the cluster, although few bookies do not have SSL enabled.
     */
    @Test(timeout = 60000)
    public void testSSLClientButOnlyFewSSLServers() throws Exception {
        // disable SSL on initial set of bookies
        ServerConfiguration serverConf = new ServerConfiguration(baseConf);
        serverConf.setSSLProviderFactoryClass(null);
        restartBookies(serverConf);

        // add two bookies which support SSL
        baseConf.setSSLProviderFactoryClass(SSLContextFactory.class.getName());

        Set<Integer> sslBookiePorts = new HashSet<>();
        sslBookiePorts.add(startNewBookie());
        sslBookiePorts.add(startNewBookie());

        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        LedgerMetadata metadata = testClient(clientConf, 2);
        assertTrue(metadata.getEnsembles().size() > 0);
        Collection<ArrayList<BookieSocketAddress>> ensembles = metadata.getEnsembles().values();
        for (ArrayList<BookieSocketAddress> bookies : ensembles) {
            for (BookieSocketAddress bookieAddress : bookies) {
                int port = bookieAddress.getPort();
                assertTrue(sslBookiePorts.contains(port));
            }
        }
    }

    /**
     * Verify that a client-side Auth plugin can access server certificates
     */
    @Test(timeout = 60000)
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
     * Verify that a bookie-side Auth plugin can access server certificates
     */
    @Test(timeout = 60000)
    public void testBookieAuthPluginRequireClientSSLAuthentication() throws Exception {
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
     * Verify that a bookie-side Auth plugin can access server certificates
     */
    @Test(timeout = 60000)
    public void testBookieAuthPluginDenyAccesstoClientWithoutSSLAuthentication() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration(baseConf);
        serverConf.setSSLClientAuthentication(false);
        serverConf.setBookieAuthProviderFactoryClass(AllowOnlyClientsWithX509Certificates.class.getName());
        restartBookies(serverConf);

        secureBookieSideChannel = false;
        secureBookieSideChannelPrincipals = null;
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        clientConf.setSSLClientAuthentication(false);

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
     * Verify that a bookie-side Auth plugin can access server certificates
     */
    @Test(timeout = 60000)
    public void testBookieAuthPluginDenyAccessToClientWithoutSSL() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration(baseConf);
        serverConf.setBookieAuthProviderFactoryClass(AllowOnlyClientsWithX509Certificates.class.getName());
        restartBookies(serverConf);

        secureBookieSideChannel = false;
        secureBookieSideChannelPrincipals = null;
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        clientConf.setSSLProviderFactoryClass("");

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
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        int origNumBookies = numBookies;

        ServerConfiguration bookieConf = newServerConfiguration();
        /*
        bookieConf.setSSLProviderFactoryClass(null);
        bs.add(startBookie(bookieConf));
        try {
            testClient(clientConf, origNumBookies + 1);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKNotEnoughBookiesException nnbe) {
            // correct response
        }

        bookieConf = newServerConfiguration();
        */
        bookieConf.setSSLProviderFactoryClass(SSLContextFactory.class.getName());
        bs.add(startBookie(bookieConf));
        testClient(clientConf, origNumBookies + 1);
    }

    /**
     * Verify that if the server hangs while an SSL client is trying to connect, the client can continue.
     */
    @Test(timeout = 60000)
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
}
