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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.ClientConnectionPeer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test authentication when enabled in client and disabled in bookie.
 */
@RunWith(Parameterized.class)
public class TestAuthMixed extends BookKeeperClusterTestCase {
    static final Logger LOG = LoggerFactory.getLogger(TestAuthMixed.class);
    public static final String TEST_AUTH_PROVIDER_PLUGIN_NAME = "TestAuthProviderPlugin";

    private static final byte[] PASSWD = "testPasswd".getBytes();
    private static final byte[] ENTRY = "TestEntry".getBytes();

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

    public TestAuthMixed(ProtocolVersion protocolVersion) {
        super(1);
        this.protocolVersion = protocolVersion;
    }

    /**
     * Test with a client that sends auth and bookie that has no auth provider.
     */
    @Test
    public void testSingleMessageAuth() throws Exception {
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(SimpleClientAuthProviderFactory.class.getName());
        clientConf.setUseV2WireProtocol(protocolVersion == ProtocolVersion.ProtocolV2);

        try (BookKeeper bkc = new BookKeeper(clientConf, zkc);
                LedgerHandle l = bkc.createLedger(1, 1, DigestType.CRC32,
                        PASSWD)) {
            LOG.info(">> Created ledger: {}", l.getId());

            l.addEntry(ENTRY);

            LOG.info(">> Added entry");

            Enumeration<LedgerEntry> e = l.readEntries(0, 0);

            LOG.info(">> Read entry");

            int count = 0;
            while (e.hasMoreElements()) {
                count++;
                assertTrue("Should match what we wrote",
                        Arrays.equals(e.nextElement().getEntry(), ENTRY));
            }

            assertEquals(1, count);

            l.close();
        }
    }

    private static class SimpleClientAuthProviderFactory implements ClientAuthProvider.Factory {

        @Override
        public void init(ClientConfiguration conf) throws IOException {
        }

        @Override
        public ClientAuthProvider newProvider(ClientConnectionPeer connection,
                AuthCallbacks.GenericCallback<Void> completeCb) {
            return new ClientAuthProvider() {

                @Override
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    LOG.info("--- --- --- --- --- Get auth response from bookie: {}", m);
                    completeCb.operationComplete(BKException.Code.OK, null);
                }

                @Override
                public void close() {
                }

                @Override
                public void init(AuthCallbacks.GenericCallback<AuthToken> cb) {
                    // Send some kind of credentials
                    LOG.info("--- --- --- --- --- Initializing the client auth provider for new connection");
                    cb.operationComplete(BKException.Code.OK, AuthToken.wrap("my-password".getBytes()));
                }
            };
        }

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void close() {
        }
    }
}
