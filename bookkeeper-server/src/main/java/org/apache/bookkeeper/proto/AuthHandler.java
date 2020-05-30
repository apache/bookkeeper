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

import static org.apache.bookkeeper.auth.AuthProviderFactoryFactory.AUTHENTICATION_DISABLED_PLUGIN_NAME;

import com.google.protobuf.ByteString;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLSession;

import org.apache.bookkeeper.auth.AuthCallbacks;
import org.apache.bookkeeper.auth.AuthToken;
import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AuthMessage;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AuthHandler {
    static final Logger LOG = LoggerFactory.getLogger(AuthHandler.class);
    private static final DefaultHostnameVerifier HOSTNAME_VERIFIER = new DefaultHostnameVerifier();

    static class ServerSideHandler extends ChannelInboundHandlerAdapter {
        volatile boolean authenticated = false;
        final BookieAuthProvider.Factory authProviderFactory;
        final BookieConnectionPeer connectionPeer;
        BookieAuthProvider authProvider;

        ServerSideHandler(BookieConnectionPeer connectionPeer, BookieAuthProvider.Factory authProviderFactory) {
            this.authProviderFactory = authProviderFactory;
            this.connectionPeer = connectionPeer;
            authProvider = null;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            authProvider = authProviderFactory.newProvider(connectionPeer, new AuthHandshakeCompleteCallback());
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (authProvider != null) {
                authProvider.close();
            }
            super.channelInactive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (authProvider == null) {
                // close the channel, authProvider should only be
                // null if the other end of line is an InetSocketAddress
                // anything else is strange, and we don't want to deal
                // with it
                ctx.channel().close();
                return;
            }

            if (authenticated) {
                super.channelRead(ctx, msg);
            } else if (msg instanceof BookieProtocol.AuthRequest) { // pre-PB-client
                BookieProtocol.AuthRequest req = (BookieProtocol.AuthRequest) msg;
                assert (req.getOpCode() == BookieProtocol.AUTH);
                if (checkAuthPlugin(req.getAuthMessage(), ctx.channel())) {
                    byte[] payload = req
                        .getAuthMessage()
                        .getPayload()
                        .toByteArray();
                    authProvider.process(AuthToken.wrap(payload),
                                new AuthResponseCallbackLegacy(req, ctx.channel()));
                } else {
                    ctx.channel().close();
                }
            } else if (msg instanceof BookieProtocol.Request) {
                BookieProtocol.Request req = (BookieProtocol.Request) msg;
                if (req.getOpCode() == BookieProtocol.ADDENTRY) {
                    ctx.channel().writeAndFlush(
                            BookieProtocol.AddResponse.create(
                                    req.getProtocolVersion(), BookieProtocol.EUA,
                                    req.getLedgerId(), req.getEntryId()));
                } else if (req.getOpCode() == BookieProtocol.READENTRY) {
                    ctx.channel().writeAndFlush(
                            new BookieProtocol.ReadResponse(
                                    req.getProtocolVersion(), BookieProtocol.EUA,
                                    req.getLedgerId(), req.getEntryId()));
                } else {
                    ctx.channel().close();
                }
            } else if (msg instanceof BookkeeperProtocol.Request) { // post-PB-client
                BookkeeperProtocol.Request req = (BookkeeperProtocol.Request) msg;
                if (req.getHeader().getOperation() == BookkeeperProtocol.OperationType.AUTH
                        && req.hasAuthRequest()
                        && checkAuthPlugin(req.getAuthRequest(), ctx.channel())) {
                    byte[] payload = req
                        .getAuthRequest()
                        .getPayload()
                        .toByteArray();
                    authProvider.process(AuthToken.wrap(payload),
                            new AuthResponseCallback(req, ctx.channel(), authProviderFactory.getPluginName()));
                } else if (req.getHeader().getOperation() == BookkeeperProtocol.OperationType.START_TLS
                        && req.hasStartTLSRequest()) {
                    super.channelRead(ctx, msg);
                } else {
                    BookkeeperProtocol.Response.Builder builder = BookkeeperProtocol.Response.newBuilder()
                        .setHeader(req.getHeader())
                        .setStatus(BookkeeperProtocol.StatusCode.EUA);

                    ctx.channel().writeAndFlush(builder.build());
                }
            } else {
                // close the channel, junk coming over it
                ctx.channel().close();
            }
        }

        private boolean checkAuthPlugin(AuthMessage am, final Channel src) {
            if (!am.hasAuthPluginName() || !am.getAuthPluginName().equals(authProviderFactory.getPluginName())) {
                LOG.error("Received message from incompatible auth plugin. Local = {}," + " Remote = {}, Channel = {}",
                        authProviderFactory.getPluginName(), am.getAuthPluginName());
                return false;
            }
            return true;
        }

        static class AuthResponseCallbackLegacy implements AuthCallbacks.GenericCallback<AuthToken> {
            final BookieProtocol.AuthRequest req;
            final Channel channel;

            AuthResponseCallbackLegacy(BookieProtocol.AuthRequest req, Channel channel) {
                this.req = req;
                this.channel = channel;
            }

            @Override
            public void operationComplete(int rc, AuthToken newam) {
                if (rc != BKException.Code.OK) {
                    LOG.error("Error processing auth message, closing connection");
                    channel.close();
                    return;
                }
                AuthMessage message = AuthMessage.newBuilder().setAuthPluginName(req.authMessage.getAuthPluginName())
                        .setPayload(ByteString.copyFrom(newam.getData())).build();
                channel.writeAndFlush(new BookieProtocol.AuthResponse(req.getProtocolVersion(), message));
            }
        }

        static class AuthResponseCallback implements AuthCallbacks.GenericCallback<AuthToken> {
            final BookkeeperProtocol.Request req;
            final Channel channel;
            final String pluginName;

            AuthResponseCallback(BookkeeperProtocol.Request req, Channel channel, String pluginName) {
                this.req = req;
                this.channel = channel;
                this.pluginName = pluginName;
            }

            @Override
            public void operationComplete(int rc, AuthToken newam) {
                BookkeeperProtocol.Response.Builder builder = BookkeeperProtocol.Response.newBuilder()
                        .setHeader(req.getHeader());

                if (rc != BKException.Code.OK) {
                    LOG.error("Error processing auth message, closing connection");

                    builder.setStatus(BookkeeperProtocol.StatusCode.EUA);
                    channel.writeAndFlush(builder.build());
                    channel.close();
                    return;
                } else {
                    AuthMessage message = AuthMessage.newBuilder().setAuthPluginName(pluginName)
                            .setPayload(ByteString.copyFrom(newam.getData())).build();
                    builder.setStatus(BookkeeperProtocol.StatusCode.EOK).setAuthResponse(message);
                    channel.writeAndFlush(builder.build());
                }
            }
        }

        class AuthHandshakeCompleteCallback implements AuthCallbacks.GenericCallback<Void> {
            @Override
            public void operationComplete(int rc, Void v) {
                if (rc == BKException.Code.OK) {
                    authenticated = true;
                    LOG.info("Authentication success on server side");
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Authentication failed on server side");
                    }
                }
            }
        }
    }

    static class ClientSideHandler extends ChannelDuplexHandler {
        volatile boolean authenticated = false;
        final ClientAuthProvider.Factory authProviderFactory;
        ClientAuthProvider authProvider;
        final AtomicLong transactionIdGenerator;
        final Queue<Object> waitingForAuth = new ConcurrentLinkedQueue<>();
        final ClientConnectionPeer connectionPeer;

        private final boolean isUsingV2Protocol;

        public ClientAuthProvider getAuthProvider() {
            return authProvider;
        }

        ClientSideHandler(ClientAuthProvider.Factory authProviderFactory, AtomicLong transactionIdGenerator,
                ClientConnectionPeer connectionPeer, boolean isUsingV2Protocol) {
            this.authProviderFactory = authProviderFactory;
            this.transactionIdGenerator = transactionIdGenerator;
            this.connectionPeer = connectionPeer;
            authProvider = null;
            this.isUsingV2Protocol = isUsingV2Protocol;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            authProvider = authProviderFactory.newProvider(connectionPeer, new AuthHandshakeCompleteCallback(ctx));
            authProvider.init(new AuthRequestCallback(ctx, authProviderFactory.getPluginName()));

            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (authProvider != null) {
                authProvider.close();
            }
            super.channelInactive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            assert (authProvider != null);

            if (authenticated) {
                super.channelRead(ctx, msg);
            } else if (msg instanceof BookkeeperProtocol.Response) {
                BookkeeperProtocol.Response resp = (BookkeeperProtocol.Response) msg;
                if (null == resp.getHeader().getOperation()) {
                    LOG.info("dropping received malformed message {} from bookie {}", msg, ctx.channel());
                    // drop the message without header
                } else {
                    switch (resp.getHeader().getOperation()) {
                    case START_TLS:
                        super.channelRead(ctx, msg);
                        break;
                    case AUTH:
                        if (resp.getStatus() != BookkeeperProtocol.StatusCode.EOK) {
                            authenticationError(ctx, resp.getStatus().getNumber());
                        } else {
                            assert (resp.hasAuthResponse());
                            BookkeeperProtocol.AuthMessage am = resp.getAuthResponse();
                            if (AUTHENTICATION_DISABLED_PLUGIN_NAME.equals(am.getAuthPluginName())){
                                SocketAddress remote = ctx.channel().remoteAddress();
                                LOG.info("Authentication is not enabled."
                                    + "Considering this client {} authenticated", remote);
                                AuthHandshakeCompleteCallback cb = new AuthHandshakeCompleteCallback(ctx);
                                cb.operationComplete(BKException.Code.OK, null);
                                return;
                            }
                            byte[] payload = am.getPayload().toByteArray();
                            authProvider.process(AuthToken.wrap(payload), new AuthRequestCallback(ctx,
                                authProviderFactory.getPluginName()));
                        }
                        break;
                    default:
                        LOG.warn("dropping received message {} from bookie {}", msg, ctx.channel());
                        // else just drop the message,
                        // we're not authenticated so nothing should be coming through
                        break;
                    }
                }
            } else if (msg instanceof BookieProtocol.Response) {
                BookieProtocol.Response resp = (BookieProtocol.Response) msg;
                switch (resp.opCode) {
                case BookieProtocol.AUTH:
                    if (resp.errorCode != BookieProtocol.EOK) {
                        authenticationError(ctx, resp.errorCode);
                    } else {
                        BookkeeperProtocol.AuthMessage am = ((BookieProtocol.AuthResponse) resp).authMessage;
                        if (AUTHENTICATION_DISABLED_PLUGIN_NAME.equals(am.getAuthPluginName())) {
                            SocketAddress remote = ctx.channel().remoteAddress();
                            LOG.info("Authentication is not enabled."
                                    + "Considering this client {} authenticated", remote);
                            AuthHandshakeCompleteCallback cb = new AuthHandshakeCompleteCallback(ctx);
                            cb.operationComplete(BKException.Code.OK, null);
                            return;
                        }
                        byte[] payload = am.getPayload().toByteArray();
                        authProvider.process(AuthToken.wrap(payload), new AuthRequestCallback(ctx,
                                authProviderFactory.getPluginName()));
                    }
                    break;
                default:
                    LOG.warn("dropping received message {} from bookie {}", msg, ctx.channel());
                    // else just drop the message, we're not authenticated so nothing should be coming
                    // through
                    break;
                }
            }
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            synchronized (this) {
                if (authenticated) {
                    super.write(ctx, msg, promise);
                    super.flush(ctx);
                } else if (msg instanceof BookkeeperProtocol.Request) {
                    // let auth messages through, queue the rest
                    BookkeeperProtocol.Request req = (BookkeeperProtocol.Request) msg;
                    if (req.getHeader().getOperation()
                            == BookkeeperProtocol.OperationType.AUTH
                        || req.getHeader().getOperation() == BookkeeperProtocol.OperationType.START_TLS) {
                        super.write(ctx, msg, promise);
                        super.flush(ctx);
                    } else {
                        waitingForAuth.add(msg);
                    }
                } else if (msg instanceof BookieProtocol.Request) {
                    // let auth messages through, queue the rest
                    BookieProtocol.Request req = (BookieProtocol.Request) msg;
                    if (BookieProtocol.AUTH == req.getOpCode()) {
                        super.write(ctx, msg, promise);
                        super.flush(ctx);
                    } else {
                        waitingForAuth.add(msg);
                    }
                } else {
                    LOG.info("dropping write of message {}", msg);
                }
            }
        }

        long newTxnId() {
            return transactionIdGenerator.incrementAndGet();
        }

        void authenticationError(ChannelHandlerContext ctx, int errorCode) {
            LOG.error("Error processing auth message, erroring connection {}", errorCode);
            ctx.fireExceptionCaught(new AuthenticationException("Auth failed with error " + errorCode));
        }

        class AuthRequestCallback implements AuthCallbacks.GenericCallback<AuthToken> {
            Channel channel;
            ChannelHandlerContext ctx;
            String pluginName;

            AuthRequestCallback(ChannelHandlerContext ctx, String pluginName) {
                this.channel = ctx.channel();
                this.ctx = ctx;
                this.pluginName = pluginName;
            }

            @Override
            public void operationComplete(int rc, AuthToken newam) {
                if (rc != BKException.Code.OK) {
                    authenticationError(ctx, rc);
                    return;
                }

                AuthMessage message = AuthMessage.newBuilder().setAuthPluginName(pluginName)
                        .setPayload(ByteString.copyFrom(newam.getData())).build();

                if (isUsingV2Protocol) {
                    channel.writeAndFlush(
                            new BookieProtocol.AuthRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, message),
                            channel.voidPromise());
                } else {
                    // V3 protocol
                    BookkeeperProtocol.BKPacketHeader header = BookkeeperProtocol.BKPacketHeader.newBuilder()
                            .setVersion(BookkeeperProtocol.ProtocolVersion.VERSION_THREE)
                            .setOperation(BookkeeperProtocol.OperationType.AUTH).setTxnId(newTxnId()).build();
                    BookkeeperProtocol.Request.Builder builder = BookkeeperProtocol.Request.newBuilder()
                            .setHeader(header)
                            .setAuthRequest(message);
                    channel.writeAndFlush(builder.build());
                }
            }
        }

        class AuthHandshakeCompleteCallback implements AuthCallbacks.GenericCallback<Void> {
            ChannelHandlerContext ctx;

            AuthHandshakeCompleteCallback(ChannelHandlerContext ctx) {
                this.ctx = ctx;
            }

            @Override
            public void operationComplete(int rc, Void v) {
                if (rc == BKException.Code.OK) {
                    synchronized (this) {
                        authenticated = true;
                        Object msg = waitingForAuth.poll();
                        while (msg != null) {
                            ctx.writeAndFlush(msg);
                            msg = waitingForAuth.poll();
                        }
                    }
                } else {
                    LOG.warn("Client authentication failed");
                    authenticationError(ctx, rc);
                }
            }
        }

        public boolean verifyTlsHostName(Channel channel) {
            SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
            if (sslHandler == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("can't perform hostname-verification on non-ssl channel {}", channel);
                }
                return true;
            }
            SSLSession sslSession = sslHandler.engine().getSession();
            String hostname = null;
            if (channel.remoteAddress() instanceof InetSocketAddress) {
                hostname = ((InetSocketAddress) channel.remoteAddress()).getHostName();
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("can't get remote hostName on ssl session {}", channel);
                }
                return true;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Verifying HostName for {}, Cipher {}, Protocols {}, on {}", hostname,
                        sslSession.getCipherSuite(), sslSession.getProtocol(), channel);
            }
            boolean verification = HOSTNAME_VERIFIER.verify(hostname, sslSession);
            if (!verification) {
                LOG.warn("Failed to validate hostname verification {} on {}", hostname, channel);
            }
            return verification;
        }
    }

    @SuppressWarnings("serial")
    static class AuthenticationException extends IOException {
        AuthenticationException(String reason) {
            super(reason);
        }
    }
}
