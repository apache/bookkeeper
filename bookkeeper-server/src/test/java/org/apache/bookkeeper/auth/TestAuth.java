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
package org.apache.bookkeeper.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieConnectionPeer;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.ClientConnectionPeer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test authentication.
 */
@RunWith(Parameterized.class)
public class TestAuth extends BookKeeperClusterTestCase {
    static final Logger LOG = LoggerFactory.getLogger(TestAuth.class);
    public static final String TEST_AUTH_PROVIDER_PLUGIN_NAME = "TestAuthProviderPlugin";
    private static final byte[] PASSWD = "testPasswd".getBytes();
    private static final byte[] ENTRY = "TestEntry".getBytes();

    private static final byte[] SUCCESS_RESPONSE = {1};
    private static final byte[] FAILURE_RESPONSE = {2};
    private static final byte[] PAYLOAD_MESSAGE = {3};

    enum ProtocolVersion {
        ProtocolV2, ProtocolV3
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                { ProtocolVersion.ProtocolV2 },
                { ProtocolVersion.ProtocolV3 },
        });
    }

    private final ProtocolVersion protocolVersion;

    public TestAuth(ProtocolVersion protocolVersion) {
        super(0); // start them later when auth providers are configured
        this.protocolVersion = protocolVersion;
    }

    protected ClientConfiguration newClientConfiguration() {
        ClientConfiguration conf = super.newClientConfiguration();
        conf.setUseV2WireProtocol(protocolVersion == ProtocolVersion.ProtocolV2);
        return conf;
    }

    // we pass in ledgerId because the method may throw exceptions
    private void connectAndWriteToBookie(ClientConfiguration conf, AtomicLong ledgerWritten)
            throws Exception {
        LOG.info("Connecting to bookie");
        try (BookKeeper bkc = new BookKeeper(conf, zkc);
            LedgerHandle l = bkc.createLedger(1, 1, DigestType.CRC32,
                    PASSWD)) {
            ledgerWritten.set(l.getId());
            l.addEntry(ENTRY);
        }
    }

    /**
     * check if the entry exists. Restart the bookie to allow
     * access
     */
    private int entryCount(long ledgerId, ServerConfiguration bookieConf,
                           ClientConfiguration clientConf) throws Exception {
        LOG.info("Counting entries in {}", ledgerId);
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        restartBookies(c -> {
                c.setBookieAuthProviderFactoryClass(
                        AlwaysSucceedBookieAuthProviderFactory.class.getName());
                return c;
            });

        int count = 0;
        try (BookKeeper bkc = new BookKeeper(clientConf, zkc);
            LedgerHandle lh = bkc.openLedger(ledgerId, DigestType.CRC32,
                                         PASSWD)) {
            if (lh.getLastAddConfirmed() < 0) {
                return 0;
            }
            Enumeration<LedgerEntry> e = lh.readEntries(0, lh.getLastAddConfirmed());
            while (e.hasMoreElements()) {
                count++;
                assertTrue("Should match what we wrote",
                        Arrays.equals(e.nextElement().getEntry(), ENTRY));
            }
        }
        return count;
    }

    /**
     * Test an connection will authorize with a single message
     * to the server and a single response.
     */
    @Test
    public void testSingleMessageAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
            AlwaysSucceedBookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
            SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        connectAndWriteToBookie(clientConf, ledgerId); // should succeed

        assertFalse(ledgerId.get() == -1);
        assertEquals("Should have entry", 1, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    @Test
    public void testCloseMethodCalledOnAuthProvider() throws Exception {
        LogCloseCallsBookieAuthProviderFactory.closeCountersOnFactory.set(0);
        LogCloseCallsBookieAuthProviderFactory.closeCountersOnConnections.set(0);
        LogCloseCallsBookieAuthProviderFactory.initCountersOnFactory.set(0);
        LogCloseCallsBookieAuthProviderFactory.initCountersOnConnections.set(0);
        LogCloseCallsClientAuthProviderFactory.initCountersOnFactory.set(0);
        LogCloseCallsClientAuthProviderFactory.closeCountersOnFactory.set(0);

        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                LogCloseCallsBookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                LogCloseCallsClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        connectAndWriteToBookie(clientConf, ledgerId); // should succeed

        assertFalse(ledgerId.get() == -1);
        assertEquals("Should have entry", 1, entryCount(ledgerId.get(), bookieConf, clientConf));

        stopAllBookies();

        assertEquals(LogCloseCallsBookieAuthProviderFactory.initCountersOnConnections.get(),
            LogCloseCallsBookieAuthProviderFactory.closeCountersOnConnections.get());
        assertTrue(LogCloseCallsBookieAuthProviderFactory.initCountersOnConnections.get() > 0);

        assertEquals(1, LogCloseCallsBookieAuthProviderFactory.initCountersOnFactory.get());
        assertEquals(1, LogCloseCallsBookieAuthProviderFactory.closeCountersOnFactory.get());

        assertEquals(LogCloseCallsClientAuthProviderFactory.initCountersOnConnections.get(),
            LogCloseCallsClientAuthProviderFactory.closeCountersOnConnections.get());
        assertTrue(LogCloseCallsClientAuthProviderFactory.initCountersOnConnections.get() > 0);

        assertEquals(1, LogCloseCallsClientAuthProviderFactory.initCountersOnFactory.get());
        assertEquals(1, LogCloseCallsClientAuthProviderFactory.closeCountersOnFactory.get());

    }

    /**
     * Test that when the bookie provider sends a failure message
     * the client will not be able to write.
     */
    @Test
    public void testSingleMessageAuthFailure() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                AlwaysFailBookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId); // should fail
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // client shouldnt be able to find enough bookies to
            // write
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals("Shouldn't have entry", 0, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    /**
     * Test that authentication works when the providers
     * exchange multiple messages.
     */
    @Test
    public void testMultiMessageAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                SucceedAfter3BookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        AtomicLong ledgerId = new AtomicLong(-1);
        startAndStoreBookie(bookieConf);
        connectAndWriteToBookie(clientConf, ledgerId); // should succeed

        assertFalse(ledgerId.get() == -1);
        assertEquals("Should have entry", 1, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    /**
     * Test that when the bookie provider sends a failure message
     * the client will not be able to write.
     */
    @Test
    public void testMultiMessageAuthFailure() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                FailAfter3BookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId); // should fail
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // bookie should have sent a negative response before
            // breaking the connection
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals("Shouldn't have entry", 0, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    /**
     * Test that when the bookie and the client have a different
     * plugin configured, no messages will get through.
     */
    @Test
    public void testDifferentPluginFailure() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                DifferentPluginBookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);
        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId); // should fail
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // bookie should have sent a negative response before
            // breaking the connection
            assertEquals(ProtocolVersion.ProtocolV3, protocolVersion);
        } catch (BKException.BKNotEnoughBookiesException nebe) {
            // With V2 we don't get the authorization error, but rather just
            // fail to write to bookies.
            assertEquals(ProtocolVersion.ProtocolV2, protocolVersion);
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals("Shouldn't have entry", 0, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    /**
     * Test that when the plugin class does exist, but
     * doesn't implement the interface, we fail predictably.
     */
    @Test
    public void testExistentButNotValidPlugin() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                "java.lang.String");

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                "java.lang.String");
        try {
            startAndStoreBookie(bookieConf);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertTrue("Wrong exception thrown",
                    e.getMessage().contains("not "
                            + BookieAuthProvider.Factory.class.getName()));
        }

        try {
            BookKeeper bkc = new BookKeeper(clientConf, zkc);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertTrue("Wrong exception thrown",
                    e.getMessage().contains("not "
                            + ClientAuthProvider.Factory.class.getName()));
        }
    }

    /**
     * Test that when the plugin class does not exist,
     * the bookie will not start and the client will
     * break.
     */
    @Test
    public void testNonExistentPlugin() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                "NonExistentClassNameForTestingAuthPlugins");

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                "NonExistentClassNameForTestingAuthPlugins");
        try {
            startAndStoreBookie(bookieConf);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertEquals("Wrong exception thrown",
                    e.getCause().getClass(), ClassNotFoundException.class);
        }

        try {
            BookKeeper bkc = new BookKeeper(clientConf, zkc);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertEquals("Wrong exception thrown",
                    e.getCause().getClass(), ClassNotFoundException.class);
        }
    }

    /**
     * Test that when the plugin on the bookie crashes, the client doesn't
     * hang also, but it cannot write in any case.
     */
    @Test
    public void testCrashDuringAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                CrashAfter3BookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId);
            fail("Shouldn't get this far");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // bookie won't respond, request will timeout, and then
            // we wont be able to find a replacement
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals("Shouldn't have entry", 0, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    /**
     * Test that when a bookie simply stops replying during auth, the client doesn't
     * hang also, but it cannot write in any case.
     */
    @Test
    public void testCrashType2DuringAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                CrashType2After3BookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        crashType2bookieInstance = startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId);
            fail("Shouldn't get this far");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // bookie won't respond, request will timeout, and then
            // we wont be able to find a replacement
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals("Shouldn't have entry", 0, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    /**
     * Client will try to perform authentication but bookies are not configured.
     */
    @Test
    public void testClientWithAuthAndBookieWithDisabledAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        assertNull(bookieConf.getBookieAuthProviderFactoryClass());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        connectAndWriteToBookie(clientConf, ledgerId); // should succeed

        assertFalse(ledgerId.get() == -1);
        assertEquals("Should have entry", 1, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    /**
     * The plugin will drop the connection from the bookie side.
     */
    @Test
    public void testDropConnectionFromBookieAuthPlugin() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                DropConnectionBookieAuthProviderFactory.class.getName());

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId); // should fail
            fail();
        } catch (BKNotEnoughBookiesException error){
        }
    }

    BookieServer startAndStoreBookie(ServerConfiguration conf) throws Exception {
        return startAndAddBookie(conf).getServer();
    }

    /**
     * Factory for a bookie that always succeeds.
     */
    public static class AlwaysSucceedBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    addr.setAuthorizedId(new BookKeeperPrincipal("test-principal"));
                    cb.operationComplete(BKException.Code.OK, AuthToken.wrap(SUCCESS_RESPONSE));
                    completeCb.operationComplete(BKException.Code.OK, null);
                }
            };
        }
    }

    private static class LogCloseCallsBookieAuthProviderFactory implements BookieAuthProvider.Factory {

        private static AtomicInteger closeCountersOnFactory = new AtomicInteger();
        private static AtomicInteger closeCountersOnConnections = new AtomicInteger();
        private static AtomicInteger initCountersOnFactory = new AtomicInteger();
        private static AtomicInteger initCountersOnConnections = new AtomicInteger();

        @Override
        public void init(ServerConfiguration conf) throws IOException {
            initCountersOnFactory.incrementAndGet();
        }

        @Override
        public void close() {
            closeCountersOnFactory.incrementAndGet();
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer connection,
                AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                {
                    completeCb.operationComplete(BKException.Code.OK, null);
                    initCountersOnConnections.incrementAndGet();
                }

                @Override
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                }

                @Override
                public void close() {
                    closeCountersOnConnections.incrementAndGet();
                }
            };
        }

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

    }

    /**
     * Factory for a bookie that drops connections.
     */
    public static class DropConnectionBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    addr.disconnect();
                }
            };
        }
    }

    /**
     * Factory for a bookie that always fails.
     */
    public static class AlwaysFailBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    addr.setAuthorizedId(new BookKeeperPrincipal("test-principal"));
                    cb.operationComplete(BKException.Code.OK, AuthToken.wrap(FAILURE_RESPONSE));
                    completeCb.operationComplete(
                            BKException.Code.UnauthorizedAccessException, null);
                }
            };
        }
    }

    private static class LogCloseCallsClientAuthProviderFactory implements ClientAuthProvider.Factory {

        private static AtomicInteger initCountersOnFactory = new AtomicInteger();
        private static AtomicInteger initCountersOnConnections = new AtomicInteger();
        private static AtomicInteger closeCountersOnFactory = new AtomicInteger();
        private static AtomicInteger closeCountersOnConnections = new AtomicInteger();

        @Override
        public void init(ClientConfiguration conf) throws IOException {
            initCountersOnFactory.incrementAndGet();
        }

        @Override
        public ClientAuthProvider newProvider(ClientConnectionPeer connection,
                AuthCallbacks.GenericCallback<Void> completeCb) {
            return new ClientAuthProvider() {

                @Override
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                }

                @Override
                public void close() {
                    closeCountersOnConnections.incrementAndGet();
                }

                @Override
                public void init(AuthCallbacks.GenericCallback<AuthToken> cb) {
                    initCountersOnConnections.incrementAndGet();
                    completeCb.operationComplete(BKException.Code.OK, null);
                }
            };
        }

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void close() {
            closeCountersOnFactory.incrementAndGet();
        }

    }

    /**
     * Factory for bookie that will send until complete.
     */
    private static class SendUntilCompleteClientAuthProviderFactory
        implements ClientAuthProvider.Factory {

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ClientConfiguration conf) {
        }

        @Override
        public ClientAuthProvider newProvider(ClientConnectionPeer addr,
                final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new ClientAuthProvider() {
                public void init(AuthCallbacks.GenericCallback<AuthToken> cb) {
                    cb.operationComplete(BKException.Code.OK, AuthToken.wrap(PAYLOAD_MESSAGE));
                }
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    byte[] type = m.getData();
                    if (Arrays.equals(type, SUCCESS_RESPONSE)) {
                        addr.setAuthorizedId(new BookKeeperPrincipal("test-client-principal"));
                        completeCb.operationComplete(BKException.Code.OK, null);
                    } else if (Arrays.equals(type, FAILURE_RESPONSE)) {
                        completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
                    } else {
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(PAYLOAD_MESSAGE));
                    }
                }
            };
        }
    }

    /**
     * Factory for bookie that succeeds after three messages.
     */
    public static class SucceedAfter3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    if (numMessages.incrementAndGet() == 3) {
                        addr.setAuthorizedId(new BookKeeperPrincipal("test-principal"));
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(SUCCESS_RESPONSE));
                        completeCb.operationComplete(BKException.Code.OK, null);
                    } else {
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(PAYLOAD_MESSAGE));
                    }
                }
            };
        }
    }

    /**
     * Factory for bookie that fails after three messages.
     */
    public static class FailAfter3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    if (numMessages.incrementAndGet() == 3) {
                        addr.setAuthorizedId(new BookKeeperPrincipal("test-principal"));
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(FAILURE_RESPONSE));
                        completeCb.operationComplete(BKException.Code.UnauthorizedAccessException,
                                                     null);
                    } else {
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(PAYLOAD_MESSAGE));
                    }
                }
            };
        }
    }

    /**
     * Factory for crashing the bookie after 3 messages with an auth provider.
     */
    public static class CrashAfter3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    if (numMessages.incrementAndGet() == 3) {
                        throw new RuntimeException("Do bad things to the bookie");
                    } else {
                        addr.setAuthorizedId(new BookKeeperPrincipal("test-principal"));
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(PAYLOAD_MESSAGE));
                    }
                }
            };
        }
    }

    private static BookieServer crashType2bookieInstance = null;
    /**
     * Factory for a bookie with CrashType2 after three messages.
     */
    public static class CrashType2After3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    if (numMessages.incrementAndGet() != 3) {
                        cb.operationComplete(BKException.Code.OK, AuthToken.wrap(PAYLOAD_MESSAGE));
                        return;
                    }
                    crashType2bookieInstance.suspendProcessing();
                }
            };
        }
    }

    /**
     * Factory for a DifferentAuthProviderPlugin.
     */
    public static class DifferentPluginBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return "DifferentAuthProviderPlugin";
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    cb.operationComplete(BKException.Code.OK, AuthToken.wrap(FAILURE_RESPONSE));
                    completeCb.operationComplete(BKException.Code.OK, null);
                }
            };
        }
    }

}
