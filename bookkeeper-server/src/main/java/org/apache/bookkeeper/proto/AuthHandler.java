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

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AuthMessage;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.DefaultExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.local.LocalChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AuthHandler {
    static final Logger LOG = LoggerFactory.getLogger(AuthHandler.class);

    static class ServerSideHandler extends SimpleChannelHandler {
        volatile boolean authenticated = false;
        final BookieAuthProvider.Factory authProviderFactory;
        BookieAuthProvider authProvider;

        ServerSideHandler(BookieAuthProvider.Factory authProviderFactory) {
            this.authProviderFactory = authProviderFactory;
            authProvider = null;
        }

        @Override
        public void channelOpen(ChannelHandlerContext ctx,
                                ChannelStateEvent e) throws Exception {
            LOG.info("Channel open {}", ctx.getChannel());
            SocketAddress remote  = ctx.getChannel().getRemoteAddress();
            if (remote instanceof InetSocketAddress) {
                authProvider = authProviderFactory.newProvider((InetSocketAddress)remote,
                        new AuthHandshakeCompleteCallback());
            } else if (ctx.getChannel() instanceof LocalChannel) {
                authProvider = authProviderFactory.newProvider(new InetSocketAddress(Inet4Address.getLocalHost(), 0),
                        new AuthHandshakeCompleteCallback());
            } else {
                LOG.error("Unknown channel ({}) or socket type {} for {}",
                        new Object[] { ctx.getChannel(), remote != null ? remote.getClass() : null, remote });
            }
            super.channelOpen(ctx, e);
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx,
                                    MessageEvent e)
                throws Exception {
            if (authProvider == null) {
                // close the channel, authProvider should only be
                // null if the other end of line is an InetSocketAddress
                // anything else is strange, and we don't want to deal
                // with it
                ctx.getChannel().close();
                return;
            }

            Object event = e.getMessage();
            if (authenticated) {
                super.messageReceived(ctx, e);
            } else if (event instanceof BookieProtocol.AuthRequest) { // pre-PB-client
                BookieProtocol.AuthRequest req = (BookieProtocol.AuthRequest)event;
                assert (req.getOpCode() == BookieProtocol.AUTH);
                if (checkAuthPlugin(req.getAuthMessage(), ctx.getChannel())) {
                    authProvider.process(req.getAuthMessage(),
                                new AuthResponseCallbackLegacy(req, ctx.getChannel()));
                } else {
                    ctx.getChannel().close();
                }
            } else if (event instanceof BookieProtocol.Request) {
                BookieProtocol.Request req = (BookieProtocol.Request)event;
                if (req.getOpCode() == BookieProtocol.ADDENTRY) {
                    ctx.getChannel().write(
                            new BookieProtocol.AddResponse(
                                    req.getProtocolVersion(), BookieProtocol.EUA,
                                    req.getLedgerId(), req.getEntryId()));
                } else if (req.getOpCode() == BookieProtocol.READENTRY) {
                    ctx.getChannel().write(
                            new BookieProtocol.ReadResponse(
                                    req.getProtocolVersion(), BookieProtocol.EUA,
                                    req.getLedgerId(), req.getEntryId()));
                } else {
                    ctx.getChannel().close();
                }
            } else if (event instanceof BookkeeperProtocol.Request) { // post-PB-client
                BookkeeperProtocol.Request req = (BookkeeperProtocol.Request)event;
                if (req.getHeader().getOperation() == BookkeeperProtocol.OperationType.AUTH
                        && req.hasAuthRequest()
                        && checkAuthPlugin(req.getAuthRequest(), ctx.getChannel())) {
                    authProvider.process(req.getAuthRequest(),
                                         new AuthResponseCallback(req, ctx.getChannel()));
                } else {
                    BookkeeperProtocol.Response.Builder builder
                        = BookkeeperProtocol.Response.newBuilder()
                        .setHeader(req.getHeader())
                        .setStatus(BookkeeperProtocol.StatusCode.EUA);

                    ctx.getChannel().write(builder.build());
                }
            } else {
                // close the channel, junk coming over it
                ctx.getChannel().close();
            }
        }

        private boolean checkAuthPlugin(AuthMessage am, final Channel src) {
            if (!am.hasAuthPluginName()
                || !am.getAuthPluginName().equals(authProviderFactory.getPluginName())) {
                LOG.error("Received message from incompatible auth plugin. Local = {},"
                          + " Remote = {}, Channel = {}",
                          authProviderFactory.getPluginName(), am.getAuthPluginName());
                return false;
            }
            return true;
        }

        static class AuthResponseCallbackLegacy implements GenericCallback<AuthMessage> {
            final BookieProtocol.AuthRequest req;
            final Channel channel;

            AuthResponseCallbackLegacy(BookieProtocol.AuthRequest req, Channel channel) {
                this.req = req;
                this.channel = channel;
            }

            public void operationComplete(int rc, AuthMessage newam) {
                if (rc != BKException.Code.OK) {
                    LOG.error("Error processing auth message, closing connection");
                    channel.close();
                    return;
                }
                channel.write(new BookieProtocol.AuthResponse(req.getProtocolVersion(),
                                                              newam));
            }
        }

        static class AuthResponseCallback implements GenericCallback<AuthMessage> {
            final BookkeeperProtocol.Request req;
            final Channel channel;

            AuthResponseCallback(BookkeeperProtocol.Request req, Channel channel) {
                this.req = req;
                this.channel = channel;
            }

            public void operationComplete(int rc, AuthMessage newam) {
                BookkeeperProtocol.Response.Builder builder
                    = BookkeeperProtocol.Response.newBuilder()
                    .setHeader(req.getHeader());

                if (rc != BKException.Code.OK) {
                    LOG.error("Error processing auth message, closing connection");

                    builder.setStatus(BookkeeperProtocol.StatusCode.EUA);
                    channel.write(builder.build());
                    channel.close();
                    return;
                } else {
                    builder.setStatus(BookkeeperProtocol.StatusCode.EOK)
                        .setAuthResponse(newam);
                    channel.write(builder.build());
                }
            }
        }

        class AuthHandshakeCompleteCallback implements GenericCallback<Void> {
            @Override
            public void operationComplete(int rc, Void v) {
                if (rc == BKException.Code.OK) {
                    authenticated = true;
                } else {
                    LOG.debug("Authentication failed on server side");
                }
            }
        }
    }

    static class ClientSideHandler extends SimpleChannelHandler {
        volatile boolean authenticated = false;
        final ClientAuthProvider.Factory authProviderFactory;
        ClientAuthProvider authProvider;
        AtomicLong transactionIdGenerator;
        Queue<MessageEvent> waitingForAuth = new ConcurrentLinkedQueue<MessageEvent>();

        ClientSideHandler(ClientAuthProvider.Factory authProviderFactory,
                          AtomicLong transactionIdGenerator) {
            this.authProviderFactory = authProviderFactory;
            this.transactionIdGenerator = transactionIdGenerator;
            authProvider = null;
        }

        @Override
        public void channelConnected(ChannelHandlerContext ctx,
                                     ChannelStateEvent e)
                throws Exception {
            SocketAddress remote  = ctx.getChannel().getRemoteAddress();
            if (remote instanceof InetSocketAddress) {
                authProvider = authProviderFactory.newProvider((InetSocketAddress)remote,
                        new AuthHandshakeCompleteCallback(ctx));
            } else if (ctx.getChannel() instanceof LocalChannel) {
                authProvider = authProviderFactory.newProvider(new InetSocketAddress(Inet4Address.getLocalHost(), 0),
                        new AuthHandshakeCompleteCallback(ctx));
            } else {
                LOG.error("Unknown channel ({}) or socket type {} for {}",
                        new Object[] { ctx.getChannel(), remote != null ? remote.getClass() : null, remote });
            }

            if (authProvider != null) {
                authProvider.init(new AuthRequestCallback(ctx));
            }
            super.channelConnected(ctx, e);
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx,
                                    MessageEvent e)
                throws Exception {
            assert (authProvider != null);

            Object event = e.getMessage();

            if (authenticated) {
                super.messageReceived(ctx, e);
            } else if (event instanceof BookkeeperProtocol.Response) {
                BookkeeperProtocol.Response resp = (BookkeeperProtocol.Response)event;
                if (resp.getHeader().getOperation() == BookkeeperProtocol.OperationType.AUTH) {
                    if (resp.getStatus() != BookkeeperProtocol.StatusCode.EOK) {
                        authenticationError(ctx, resp.getStatus().getNumber());
                    } else {
                        assert (resp.hasAuthResponse());
                        BookkeeperProtocol.AuthMessage am = resp.getAuthResponse();
                        authProvider.process(am, new AuthRequestCallback(ctx));
                    }
                } else {
                    // else just drop the message,
                    // we're not authenticated so nothing should be coming through
                }
            }
        }

        @Override
        public void writeRequested(ChannelHandlerContext ctx,
                                   MessageEvent e)
                throws Exception {
            synchronized (this) {
                if (authenticated) {
                    super.writeRequested(ctx, e);
                } else if (e.getMessage() instanceof BookkeeperProtocol.Request) {
                    // let auth messages through, queue the rest
                    BookkeeperProtocol.Request req = (BookkeeperProtocol.Request)e.getMessage();
                    if (req.getHeader().getOperation()
                            == BookkeeperProtocol.OperationType.AUTH) {
                        super.writeRequested(ctx, e);
                    } else {
                        waitingForAuth.add(e);
                    }
                } // else just drop
            }
        }

        long newTxnId() {
            return transactionIdGenerator.incrementAndGet();
        }

        void authenticationError(ChannelHandlerContext ctx, int errorCode) {
            LOG.error("Error processing auth message, erroring connection {}", errorCode);
            ctx.sendUpstream(new DefaultExceptionEvent(ctx.getChannel(),
                                     new AuthenticationException(
                                             "Auth failed with error " + errorCode)));
        }

        class AuthRequestCallback implements GenericCallback<AuthMessage> {
            Channel channel;
            ChannelHandlerContext ctx;

            AuthRequestCallback(ChannelHandlerContext ctx) {
                this.channel = ctx.getChannel();
                this.ctx = ctx;
            }

            public void operationComplete(int rc, AuthMessage newam) {
                if (rc != BKException.Code.OK) {
                    authenticationError(ctx, rc);
                    return;
                }

                BookkeeperProtocol.BKPacketHeader header
                    = BookkeeperProtocol.BKPacketHeader.newBuilder()
                    .setVersion(BookkeeperProtocol.ProtocolVersion.VERSION_THREE)
                    .setOperation(BookkeeperProtocol.OperationType.AUTH)
                    .setTxnId(newTxnId()).build();
                BookkeeperProtocol.Request.Builder builder
                    = BookkeeperProtocol.Request.newBuilder()
                    .setHeader(header)
                    .setAuthRequest(newam);

                channel.write(builder.build());
            }
        }

        class AuthHandshakeCompleteCallback implements GenericCallback<Void> {
            ChannelHandlerContext ctx;
            AuthHandshakeCompleteCallback(ChannelHandlerContext ctx) {
                this.ctx = ctx;
            }

            @Override
            public void operationComplete(int rc, Void v) {
                if (rc == BKException.Code.OK) {
                    synchronized (this) {
                        authenticated = true;
                        MessageEvent e = waitingForAuth.poll();
                        while (e != null) {
                            ctx.sendDownstream(e);
                            e = waitingForAuth.poll();
                        }
                    }
                } else {
                    authenticationError(ctx, rc);
                    LOG.debug("Authentication failed on server side");
                }
            }
        }
    }

    static class AuthenticationException extends IOException {
        AuthenticationException(String reason) {
            super(reason);
        }
    }
}
