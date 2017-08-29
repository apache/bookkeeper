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

import org.junit.*;

import java.util.concurrent.CountDownLatch;
import java.util.Enumeration;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.TLSContextFactory;
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
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.proto.BookieConnectionPeer;
import org.apache.bookkeeper.proto.ClientConnectionPeer;
import org.apache.bookkeeper.proto.TestPerChannelBookieClient;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests with TLS enabled.
 */
public class TestTLS extends BookKeeperClusterTestCase {

    static Logger LOG = LoggerFactory.getLogger(TestPerChannelBookieClient.class);

    private static boolean secureClientSideChannel = false;
    private static Collection<Object> secureClientSideChannelPrincipals = null;

    private static boolean secureBookieSideChannel = false;
    private static Collection<Object> secureBookieSideChannelPrincipals = null;


    public TestTLS() {
        super(3);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        /* client configuration */
        baseClientConf.setTLSProviderFactoryClass(TLSContextFactory.class.getName());
        baseClientConf.setTLSClientAuthentication(true);
        baseClientConf.setTLSKeyStoreType("JKS");
        baseClientConf.setTLSKeyStore(this.getClass().getClassLoader().getResource("client.jks").getPath());
        baseClientConf.setTLSKeyStorePasswordPath(
                this.getClass().getClassLoader().getResource("keyStoreClientPassword.txt").getPath());
        baseClientConf.setTLSTrustStoreType("JKS");
        baseClientConf.setTLSTrustStore(this.getClass().getClassLoader().getResource("cacerts").getPath());
        baseClientConf.setTLSTrustStorePasswordPath(
                this.getClass().getClassLoader().getResource("trustStorePassword.txt").getPath());

        /* server configuration */
        baseConf.setTLSProviderFactoryClass(TLSContextFactory.class.getName());
        baseConf.setTLSClientAuthentication(true);
        baseConf.setTLSKeyStoreType("JKS");
        baseConf.setTLSKeyStore(this.getClass().getClassLoader().getResource("server.jks").getPath());
        baseConf.setTLSKeyStorePasswordPath(
                this.getClass().getClassLoader().getResource("keyStoreServerPassword.txt").getPath());
        baseConf.setTLSTrustStoreType("JKS");
        baseConf.setTLSTrustStore(this.getClass().getClassLoader().getResource("cacerts").getPath());
        baseConf.setTLSTrustStorePasswordPath(
                this.getClass().getClassLoader().getResource("trustStorePassword.txt").getPath());

        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Verify that a server will not start if tls is enabled but no cert is specified
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
     * Verify that a server will not start if tls is enabled but the cert password is incorrect
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
     * Verify the basic use of TLS. TLS client, TLS servers
     */
    @Test
    public void testConnectToTLSClusterTLSClient() throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        testClient(clientConf, numBookies);
    }


    /**
     * Multiple clients, some with TLS, and some without TLS
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
     * Verify that a client without tls enabled can connect to a cluster with TLS
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
        assertTrue(metadata.getEnsembles().size() > 0);
        Collection<ArrayList<BookieSocketAddress>> ensembles = metadata.getEnsembles().values();
        for (ArrayList<BookieSocketAddress> bookies : ensembles) {
            for (BookieSocketAddress bookieAddress : bookies) {
                int port = bookieAddress.getPort();
                assertTrue(tlsBookiePorts.contains(port));
            }
        }
    }

    /**
     * Verify that a client-side Auth plugin can access server certificates
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
     * Verify that a bookie-side Auth plugin can access server certificates
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
     * Verify that a bookie-side Auth plugin can access server certificates
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
     * Verify that a bookie-side Auth plugin can access server certificates
     */
    @Test
    public void testBookieAuthPluginDenyAccessToClientWithoutTLS() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration(baseConf);
        serverConf.setBookieAuthProviderFactoryClass(AllowOnlyClientsWithX509Certificates.class.getName());
        restartBookies(serverConf);

        secureBookieSideChannel = false;
        secureBookieSideChannelPrincipals = null;
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        clientConf.setTLSProviderFactoryClass("");

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
        /*
        bookieConf.setTLSProviderFactoryClass(null);
        bs.add(startBookie(bookieConf));
        try {
            testClient(clientConf, origNumBookies + 1);
            fail("Shouldn't be able to connect");
        } catch (BKException.BKNotEnoughBookiesException nnbe) {
            // correct response
        }

        bookieConf = newServerConfiguration();
        */
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
}
