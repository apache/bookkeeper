/**
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
package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ExtensionRegistry;

import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.PerChannelBookieClient;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.auth.TestAuth;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.proto.BookieProtocol.*;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AuthMessage;

import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.Assert.*;

public class TestBackwardCompatCMS42 extends BookKeeperClusterTestCase {
    static final Logger LOG = LoggerFactory.getLogger(TestBackwardCompatCMS42.class);

    ExtensionRegistry extRegistry = ExtensionRegistry.newInstance();
    ClientAuthProvider.Factory authProvider;
    ClientSocketChannelFactory channelFactory
        = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                            Executors.newCachedThreadPool());
    OrderedSafeExecutor executor = OrderedSafeExecutor.newBuilder().numThreads(1).name("TestBackwardCompatClient")
            .build();

    public TestBackwardCompatCMS42() throws Exception {
        super(0);

        TestDataFormats.registerAllExtensions(extRegistry);
        authProvider = AuthProviderFactoryFactory.newClientAuthProviderFactory(
                new ClientConfiguration(), extRegistry);
    }

    @Test(timeout=60000)
    public void testAuthSingleMessage() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                TestAuth.AlwaysSucceedBookieAuthProviderFactory.class.getName());
        BookieServer bookie1 = startAndStoreBookie(bookieConf);

        AuthMessage.Builder builder = AuthMessage.newBuilder()
            .setAuthPluginName(TestAuth.TEST_AUTH_PROVIDER_PLUGIN_NAME);
        builder.setExtension(TestDataFormats.messageType,
                             TestDataFormats.AuthMessageType.PAYLOAD_MESSAGE);
        final AuthMessage authMessage = builder.build();

        CompatClient42 client = newCompatClient(bookie1.getLocalAddress());

        Request request = new AuthRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, authMessage);
        client.sendRequest(request);

        Response response = client.takeResponse();
        assertTrue("Should be auth response", response instanceof AuthResponse);
        assertEquals("Should have succeeded", response.getErrorCode(), BookieProtocol.EOK);
    }

    @Test(timeout=60000)
    public void testAuthMultiMessage() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                TestAuth.SucceedAfter3BookieAuthProviderFactory.class.getName());
        BookieServer bookie1 = startAndStoreBookie(bookieConf);

        AuthMessage.Builder builder = AuthMessage.newBuilder()
            .setAuthPluginName(TestAuth.TEST_AUTH_PROVIDER_PLUGIN_NAME);
        builder.setExtension(TestDataFormats.messageType,
                             TestDataFormats.AuthMessageType.PAYLOAD_MESSAGE);
        final AuthMessage authMessage = builder.build();
        CompatClient42 client = newCompatClient(bookie1.getLocalAddress());

        Request request = new AuthRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, authMessage);
        for (int i = 0; i < 3 ; i++) {
            client.sendRequest(request);
            Response response = client.takeResponse();
            assertTrue("Should be auth response", response instanceof AuthResponse);
            AuthResponse authResponse = (AuthResponse)response;
            assertEquals("Should have succeeded",
                         response.getErrorCode(), BookieProtocol.EOK);
            TestDataFormats.AuthMessageType type = authResponse.getAuthMessage()
                .getExtension(TestDataFormats.messageType);
            if (i == 2) {
                assertEquals("Should succeed after 3",
                             type, TestDataFormats.AuthMessageType.SUCCESS_RESPONSE);
            } else {
                assertEquals("Should be payload", type,
                             TestDataFormats.AuthMessageType.PAYLOAD_MESSAGE);
            }
        }
    }

    @Test(timeout=60000)
    public void testAuthFail() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                TestAuth.FailAfter3BookieAuthProviderFactory.class.getName());
        BookieServer bookie1 = startAndStoreBookie(bookieConf);

        AuthMessage.Builder builder = AuthMessage.newBuilder()
            .setAuthPluginName(TestAuth.TEST_AUTH_PROVIDER_PLUGIN_NAME);
        builder.setExtension(TestDataFormats.messageType,
                             TestDataFormats.AuthMessageType.PAYLOAD_MESSAGE);
        final AuthMessage authMessage = builder.build();
        CompatClient42 client = newCompatClient(bookie1.getLocalAddress());

        Request request = new AuthRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, authMessage);
        for (int i = 0; i < 3 ; i++) {
            client.sendRequest(request);
            Response response = client.takeResponse();
            assertTrue("Should be auth response", response instanceof AuthResponse);
            AuthResponse authResponse = (AuthResponse)response;
            assertEquals("Should have succeeded",
                         response.getErrorCode(), BookieProtocol.EOK);
            TestDataFormats.AuthMessageType type = authResponse.getAuthMessage()
                .getExtension(TestDataFormats.messageType);
            if (i == 2) {
                assertEquals("Should fail after 3",
                             type, TestDataFormats.AuthMessageType.FAILURE_RESPONSE);
            } else {
                assertEquals("Should be payload", type,
                             TestDataFormats.AuthMessageType.PAYLOAD_MESSAGE);
            }

        }

        client.sendRequest(new ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                                           1L, 1L, (short)0));
        Response response = client.takeResponse();
        assertEquals("Should have failed",
                     response.getErrorCode(), BookieProtocol.EUA);
    }

    // copy from TestAuth
    BookieServer startAndStoreBookie(ServerConfiguration conf) throws Exception {
        bsConfs.add(conf);
        BookieServer s = startBookie(conf);
        bs.add(s);
        return s;
    }

    CompatClient42 newCompatClient(BookieSocketAddress addr) throws Exception {
        return new CompatClient42(executor, channelFactory, addr, authProvider, extRegistry);
    }

    // extending PerChannelBookieClient to get the pipeline factory
    class CompatClient42 extends PerChannelBookieClient {
        final ArrayBlockingQueue<Response> responses = new ArrayBlockingQueue<Response>(10);
        final Channel channel;
        final CountDownLatch connected = new CountDownLatch(1);

        CompatClient42(OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                       BookieSocketAddress addr,
                       ClientAuthProvider.Factory authProviderFactory,
                       ExtensionRegistry extRegistry) throws Exception {
            super(executor, channelFactory, addr, authProviderFactory, extRegistry);

            ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
            bootstrap.setPipelineFactory(this);
            bootstrap.setOption("tcpNoDelay", false);
            bootstrap.setOption("keepAlive", true);
            ChannelFuture f = bootstrap.connect(addr.getSocketAddress()).await();
            channel = f.getChannel();
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            if (!(e.getMessage() instanceof Response)) {
                LOG.error("Unknown message {}, passing upstream", e.getMessage());
                ctx.sendUpstream(e);
                return;
            }
            responses.add((Response)e.getMessage());
        }

        @Override
        public void channelConnected(ChannelHandlerContext ctx,
                                     ChannelStateEvent e)
                throws Exception {
            connected.countDown();
        }

        Response takeResponse() throws Exception {
            return responses.take();
        }

        Response pollResponse() throws Exception {
            return responses.poll();
        }

        void sendRequest(Request request) throws Exception {
            connected.await();
            channel.write(request);
        }
    }
}

