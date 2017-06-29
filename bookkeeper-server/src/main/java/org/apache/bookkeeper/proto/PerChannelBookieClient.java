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
package org.apache.bookkeeper.proto;

import static org.apache.bookkeeper.client.LedgerHandle.INVALID_ENTRY_ID;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;


import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Collection;
import java.util.Queue;
import java.util.Set;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.auth.ClientAuthProvider;
import com.google.protobuf.ByteString;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperClientStats;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallbackCtx;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadLacRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadLacResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacResponse;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.protobuf.ExtensionRegistry;
import java.net.SocketAddress;

import org.apache.bookkeeper.auth.BookKeeperPrincipal;
import org.apache.bookkeeper.client.ClientConnectionPeer;

/**
 * This class manages all details of connection to a particular bookie. It also
 * has reconnect logic if a connection to a bookie fails.
 *
 */
@Sharable
public class PerChannelBookieClient extends ChannelInboundHandlerAdapter {

    static final Logger LOG = LoggerFactory.getLogger(PerChannelBookieClient.class);

    // this set contains the bookie error return codes that we do not consider for a bookie to be "faulty"
    private static final Set<Integer> expectedBkOperationErrors = Collections.unmodifiableSet(Sets
            .newHashSet(BKException.Code.BookieHandleNotAvailableException,
                        BKException.Code.NoSuchEntryException,
                        BKException.Code.NoSuchLedgerExistsException,
                        BKException.Code.LedgerFencedException,
                        BKException.Code.LedgerExistException,
                        BKException.Code.DuplicateEntryIdException,
                        BKException.Code.WriteOnReadOnlyBookieException));

    public static final AtomicLong txnIdGenerator = new AtomicLong(0);

    final BookieSocketAddress addr;
    final EventLoopGroup eventLoopGroup;
    final OrderedSafeExecutor executor;
    final HashedWheelTimer requestTimer;
    final int addEntryTimeout;
    final int readEntryTimeout;
    final int maxFrameSize;
    final int getBookieInfoTimeout;

    private final ConcurrentHashMap<CompletionKey, CompletionValue> completionObjects = new ConcurrentHashMap<CompletionKey, CompletionValue>();

    private final StatsLogger statsLogger;
    private final OpStatsLogger readEntryOpLogger;
    private final OpStatsLogger readTimeoutOpLogger;
    private final OpStatsLogger addEntryOpLogger;
    private final OpStatsLogger writeLacOpLogger;
    private final OpStatsLogger readLacOpLogger;
    private final OpStatsLogger addTimeoutOpLogger;
    private final OpStatsLogger writeLacTimeoutOpLogger;
    private final OpStatsLogger readLacTimeoutOpLogger;
    private final OpStatsLogger getBookieInfoOpLogger;
    private final OpStatsLogger getBookieInfoTimeoutOpLogger;

    private final boolean useV2WireProtocol;

    /**
     * The following member variables do not need to be concurrent, or volatile
     * because they are always updated under a lock
     */
    private volatile Queue<GenericCallback<PerChannelBookieClient>> pendingOps =
            new ArrayDeque<GenericCallback<PerChannelBookieClient>>();
    volatile Channel channel = null;
    private final ClientConnectionPeer connectionPeer;
    private volatile BookKeeperPrincipal authorizedId = BookKeeperPrincipal.ANONYMOUS;

    enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED, CLOSED
    }

    volatile ConnectionState state;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
    private final ClientConfiguration conf;

    private final PerChannelBookieClientPool pcbcPool;
    private final ClientAuthProvider.Factory authProviderFactory;
    private final ExtensionRegistry extRegistry;

    public PerChannelBookieClient(OrderedSafeExecutor executor, EventLoopGroup eventLoopGroup,
                                  BookieSocketAddress addr) {
        this(new ClientConfiguration(), executor, eventLoopGroup, addr, null, NullStatsLogger.INSTANCE, null, null,
                null);
    }

    public PerChannelBookieClient(OrderedSafeExecutor executor, EventLoopGroup eventLoopGroup,
                                  BookieSocketAddress addr,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry) {
        this(new ClientConfiguration(), executor, eventLoopGroup, addr, null, NullStatsLogger.INSTANCE,
                authProviderFactory, extRegistry, null);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedSafeExecutor executor,
            EventLoopGroup eventLoopGroup, BookieSocketAddress addr,
                                  HashedWheelTimer requestTimer, StatsLogger parentStatsLogger,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry,
                                  PerChannelBookieClientPool pcbcPool) {
        this.maxFrameSize = conf.getNettyMaxFrameSizeBytes();
        this.conf = conf;
        this.addr = addr;
        this.executor = executor;
        if (LocalBookiesRegistry.isLocalBookie(addr)) {
            this.eventLoopGroup = new DefaultEventLoopGroup();
        } else {
            this.eventLoopGroup = eventLoopGroup;
        }
        this.state = ConnectionState.DISCONNECTED;
        this.requestTimer = requestTimer;
        this.addEntryTimeout = conf.getAddEntryTimeout();
        this.readEntryTimeout = conf.getReadEntryTimeout();
        this.getBookieInfoTimeout = conf.getBookieInfoTimeout();
        this.useV2WireProtocol = conf.getUseV2WireProtocol();

        this.authProviderFactory = authProviderFactory;
        this.extRegistry = extRegistry;

        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(addr.getHostName().replace('.', '_').replace('-', '_'))
            .append("_").append(addr.getPort());

        this.statsLogger = parentStatsLogger.scope(BookKeeperClientStats.CHANNEL_SCOPE)
            .scope(nameBuilder.toString());

        readEntryOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_READ_OP);
        addEntryOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_ADD_OP);
        writeLacOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_WRITE_LAC_OP);
        readLacOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_READ_LAC_OP);
        getBookieInfoOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.GET_BOOKIE_INFO_OP);
        readTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_READ);
        addTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_ADD);
        writeLacTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_WRITE_LAC);
        readLacTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_READ_LAC);
        getBookieInfoTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.TIMEOUT_GET_BOOKIE_INFO);

        this.pcbcPool = pcbcPool;

        this.connectionPeer = new ClientConnectionPeer() {

            @Override
            public SocketAddress getRemoteAddr() {
                Channel c = channel;
                if (c != null) {
                    return c.remoteAddress();
                } else {
                    return null;
                }
            }

            @Override
            public Collection<Object> getProtocolPrincipals() {
                return Collections.emptyList();
            }

            @Override
            public void disconnect() {
                Channel c = channel;
                if (c != null) {
                    c.close();
                }
                LOG.info("authplugin disconnected channel {}", channel);
            }

            @Override
            public void setAuthorizedId(BookKeeperPrincipal principal) {
                authorizedId = principal;
                LOG.info("connection {} authenticated as {}", channel, principal);
            }

            @Override
            public BookKeeperPrincipal getAuthorizedId() {
                return authorizedId;
            }

        };
    }

    private void completeOperation(GenericCallback<PerChannelBookieClient> op, int rc) {
        //Thread.dumpStack();
        closeLock.readLock().lock();
        try {
            if (ConnectionState.CLOSED == state) {
                op.operationComplete(BKException.Code.ClientClosedException, this);
            } else {
                op.operationComplete(rc, this);
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    protected ChannelFuture connect() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connecting to bookie: {}", addr);
        }

        // Set up the ClientBootStrap so we can create a new Channel connection to the bookie.
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        if (eventLoopGroup instanceof EpollEventLoopGroup) {
            bootstrap.channel(EpollSocketChannel.class);
        } else if (eventLoopGroup instanceof DefaultEventLoopGroup) {
            bootstrap.channel(LocalChannel.class);
        } else {
            bootstrap.channel(NioSocketChannel.class);
        }

        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.getClientConnectTimeoutMillis());
        bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                conf.getClientWriteBufferLowWaterMark(), conf.getClientWriteBufferHighWaterMark()));

        if (!(eventLoopGroup instanceof DefaultEventLoopGroup)) {
            bootstrap.option(ChannelOption.TCP_NODELAY, conf.getClientTcpNoDelay());
            bootstrap.option(ChannelOption.SO_KEEPALIVE, conf.getClientSockKeepalive());

            // if buffer sizes are 0, let OS auto-tune it
            if (conf.getClientSendBufferSize() > 0) {
                bootstrap.option(ChannelOption.SO_SNDBUF, conf.getClientSendBufferSize());
            }

            if (conf.getClientReceiveBufferSize() > 0) {
                bootstrap.option(ChannelOption.SO_RCVBUF, conf.getClientReceiveBufferSize());
            }
        }

        // In the netty pipeline, we need to split packets based on length, so we
        // use the {@link LengthFieldBasedFramDecoder}. Other than that all actions
        // are carried out in this class, e.g., making sense of received messages,
        // prepending the length to outgoing packets etc.
        bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                pipeline.addLast("lengthbasedframedecoder",
                        new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
                pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
                pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.RequestEncoder(extRegistry));
                pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.ResponseDecoder(extRegistry));
                pipeline.addLast("authHandler", new AuthHandler.ClientSideHandler(authProviderFactory, txnIdGenerator, connectionPeer));
                pipeline.addLast("mainhandler", PerChannelBookieClient.this);
            }
        });

        SocketAddress bookieAddr = addr.getSocketAddress();
        if (eventLoopGroup instanceof DefaultEventLoopGroup) {
            bookieAddr = addr.getLocalAddress();
        }

        ChannelFuture future = bootstrap.connect(bookieAddr);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Channel connected ({}) {}", future.isSuccess(), future.channel());
                }
                int rc;
                Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;

                synchronized (PerChannelBookieClient.this) {
                    if (future.isSuccess() && state == ConnectionState.CONNECTING) {
                        LOG.info("Successfully connected to bookie: {}", future.channel());
                        rc = BKException.Code.OK;
                        channel = future.channel();
                        state = ConnectionState.CONNECTED;
                    } else if (future.isSuccess() && (state == ConnectionState.CLOSED
                                                      || state == ConnectionState.DISCONNECTED)) {
                        LOG.warn("Closed before connection completed, clean up: {}, current state {}",
                                future.channel(), state);
                        closeChannel(future.channel());
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        channel = null;
                    } else if (future.isSuccess() && state == ConnectionState.CONNECTED) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Already connected with another channel({}), so close the new channel({})",
                                    channel, future.channel());
                        }
                        closeChannel(future.channel());
                        return; // pendingOps should have been completed when other channel connected
                    } else {
                        LOG.error("Could not connect to bookie: {}/{}, current state {} : ",
                                new Object[] { future.channel(), addr, state, future.cause() });
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        closeChannel(future.channel());
                        channel = null;
                        if (state != ConnectionState.CLOSED) {
                            state = ConnectionState.DISCONNECTED;
                        }
                    }

                    // trick to not do operations under the lock, take the list
                    // of pending ops and assign it to a new variable, while
                    // emptying the pending ops by just assigning it to a new
                    // list
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<GenericCallback<PerChannelBookieClient>>();
                }

                for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
                    completeOperation(pendingOp, rc);
                }
            }
        });

        return future;
    }

    void connectIfNeededAndDoOp(GenericCallback<PerChannelBookieClient> op) {
        boolean completeOpNow = false;
        int opRc = BKException.Code.OK;
        // common case without lock first
        if (channel != null && state == ConnectionState.CONNECTED) {
            completeOpNow = true;
        } else {

            synchronized (this) {
                // check the channel status again under lock
                if (channel != null && state == ConnectionState.CONNECTED) {
                    completeOpNow = true;
                    opRc = BKException.Code.OK;
                } else if (state == ConnectionState.CLOSED) {
                    completeOpNow = true;
                    opRc = BKException.Code.BookieHandleNotAvailableException;
                } else {
                    // channel is either null (first connection attempt), or the
                    // channel is disconnected. Connection attempt is still in
                    // progress, queue up this op. Op will be executed when
                    // connection attempt either fails or succeeds
                    pendingOps.add(op);

                    if (state == ConnectionState.CONNECTING) {
                        // just return as connection request has already send
                        // and waiting for the response.
                        return;
                    }
                    // switch state to connecting and do connection attempt
                    state = ConnectionState.CONNECTING;
                }
            }
            if (!completeOpNow) {
                // Start connection attempt to the input server host.
                connect();
            }
        }

        if (completeOpNow) {
            completeOperation(op, opRc);
        }

    }

    void writeLac(final long ledgerId, final byte[] masterKey, final long lac, ByteBuf toSend, WriteLacCallback cb,
            Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new V3CompletionKey(txnId, OperationType.WRITE_LAC);
        // writeLac is mostly like addEntry hence uses addEntryTimeout
        completionObjects.put(completionKey,
                new WriteLacCompletion(writeLacOpLogger, cb, ctx, lac, scheduleTimeout(completionKey, addEntryTimeout)));

        // Build the request
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.WRITE_LAC)
                .setTxnId(txnId);
        WriteLacRequest.Builder writeLacBuilder = WriteLacRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setLac(lac)
                .setMasterKey(ByteString.copyFrom(masterKey))
                .setBody(ByteString.copyFrom(toSend.nioBuffer()));

        final Request writeLacRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setWriteLacRequest(writeLacBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutWriteLacKey(completionKey);
            return;
        }
        try {
            ChannelFuture future = c.writeAndFlush(writeLacRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request for writeLac LedgerId: {} bookie: {}",
                                    ledgerId, c.remoteAddress());
                        }
                    } else {
                        if (!(future.cause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing Lac(lid={} to channel {} failed : ",
                                    new Object[] { ledgerId, c, future.cause() });
                        }
                        errorOutWriteLacKey(completionKey);
                    }
                }
            });
        } catch (Throwable e) {
            LOG.warn("writeLac operation failed", e);
            errorOutWriteLacKey(completionKey);
        }
    }

    /**
     * This method should be called only after connection has been checked for
     * {@link #connectIfNeededAndDoOp(GenericCallback)}
     *
     * @param ledgerId
     *          Ledger Id
     * @param masterKey
     *          Master Key
     * @param entryId
     *          Entry Id
     * @param toSend
     *          Buffer to send
     * @param cb
     *          Write callback
     * @param ctx
     *          Write callback context
     * @param options
     *          Add options
     */
    void addEntry(final long ledgerId, byte[] masterKey, final long entryId, ByteBuf toSend, WriteCallback cb,
                  Object ctx, final int options) {
        Object request = null;
        CompletionKey completion = null;
        if (useV2WireProtocol) {
            completion = new V2CompletionKey(ledgerId, entryId, OperationType.ADD_ENTRY);
            request = new BookieProtocol.AddRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId,
                    (short) options, masterKey, toSend);
        } else {
            final long txnId = getTxnId();
            completion = new V3CompletionKey(txnId, OperationType.ADD_ENTRY);
            // Build the request and calculate the total size to be included in the packet.
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.ADD_ENTRY)
                    .setTxnId(txnId);

            byte[] toSendArray = new byte[toSend.readableBytes()];
            toSend.getBytes(toSend.readerIndex(), toSendArray);
            AddRequest.Builder addBuilder = AddRequest.newBuilder()
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId)
                    .setMasterKey(ByteString.copyFrom(masterKey))
                    .setBody(ByteString.copyFrom(toSendArray));

            if (((short) options & BookieProtocol.FLAG_RECOVERY_ADD) == BookieProtocol.FLAG_RECOVERY_ADD) {
                addBuilder.setFlag(AddRequest.Flag.RECOVERY_ADD);
            }

            request = Request.newBuilder()
                    .setHeader(headerBuilder)
                    .setAddRequest(addBuilder)
                    .build();
        }

        final Object addRequest = request;
        final CompletionKey completionKey = completion;

        completionObjects.put(completionKey, new AddCompletion(this,
                addEntryOpLogger, cb, ctx, ledgerId, entryId, scheduleTimeout(completion, addEntryTimeout)));

        final int entrySize = toSend.readableBytes();

        final Channel c = channel;
        if (c == null) {
            errorOutAddKey(completionKey);
            toSend.release();
            return;
        }
        try {
            ChannelFuture future = c.writeAndFlush(addRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request for adding entry: " + entryId + " ledger-id: " + ledgerId
                                                            + " bookie: " + c.remoteAddress() + " entry length: " + entrySize);
                        }
                        // totalBytesOutstanding.addAndGet(entrySize);
                    } else {
                        if (!(future.cause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing addEntry(lid={}, eid={}) to channel {} failed : ",
                                    new Object[] { ledgerId, entryId, c, future.cause() });
                        }
                        errorOutAddKey(completionKey);
                    }
                }
            });
        } catch (Throwable e) {
            LOG.warn("Add entry operation failed", e);
            errorOutAddKey(completionKey);
        }
    }

    public void readEntryAndFenceLedger(final long ledgerId, byte[] masterKey,
                                        final long entryId,
                                        ReadEntryCallback cb, Object ctx) {
        Object request = null;
        CompletionKey completion = null;
        if (useV2WireProtocol) {
            completion = new V2CompletionKey(ledgerId, entryId, OperationType.READ_ENTRY);
            request = new BookieProtocol.ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId,
                    BookieProtocol.FLAG_DO_FENCING, masterKey);
        } else {
            final long txnId = getTxnId();
            completion = new V3CompletionKey(txnId, OperationType.READ_ENTRY);

            // Build the request and calculate the total size to be included in the packet.
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.READ_ENTRY)
                    .setTxnId(txnId);

            ReadRequest.Builder readBuilder = ReadRequest.newBuilder()
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId)
                    .setMasterKey(ByteString.copyFrom(masterKey))
                    .setFlag(ReadRequest.Flag.FENCE_LEDGER);

            request = Request.newBuilder()
                    .setHeader(headerBuilder)
                    .setReadRequest(readBuilder)
                    .build();
        }

        final CompletionKey completionKey = completion;
        if (completionObjects.putIfAbsent(completionKey, new ReadCompletion(this, readEntryOpLogger, cb,
                ctx, ledgerId, entryId, scheduleTimeout(completionKey, readEntryTimeout))) != null) {
            // We cannot have more than 1 pending read on the same ledger/entry in the v2 protocol
            cb.readEntryComplete(BKException.Code.BookieHandleNotAvailableException, ledgerId, entryId, null, ctx);
            return;
        }

        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(completionKey);
            return;
        }

        final Object readRequest = request;
        try {
            ChannelFuture future = c.writeAndFlush(readRequest);
            future.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Successfully wrote request {} to {}",
                                          readRequest, c.remoteAddress());
                            }
                        } else {
                            if (!(future.cause() instanceof ClosedChannelException)) {
                                LOG.warn("Writing readEntryAndFenceLedger(lid={}, eid={}) to channel {} failed : ",
                                        new Object[] { ledgerId, entryId, c, future.cause() });
                            }
                            errorOutReadKey(completionKey);
                        }
                    }
                });
        } catch(Throwable e) {
            LOG.warn("Read entry operation {} failed", completionKey, e);
            errorOutReadKey(completionKey);
        }
    }

    public void readLac(final long ledgerId, ReadLacCallback cb, Object ctx) {
        Object request = null;
        CompletionKey completion = null;
        if (useV2WireProtocol) {
            request = new BookieProtocol.ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                    ledgerId, 0, (short) 0);
            completion = new V2CompletionKey(ledgerId, 0, OperationType.READ_LAC);
        } else {
            final long txnId = getTxnId();
            completion = new V3CompletionKey(txnId, OperationType.READ_LAC);

            // Build the request and calculate the total size to be included in the packet.
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.READ_LAC)
                    .setTxnId(txnId);
            ReadLacRequest.Builder readLacBuilder = ReadLacRequest.newBuilder()
                    .setLedgerId(ledgerId);
            request = Request.newBuilder()
                    .setHeader(headerBuilder)
                    .setReadLacRequest(readLacBuilder)
                    .build();
        }
        final Object readLacRequest = request;
        final CompletionKey completionKey = completion;

        completionObjects.put(completionKey,
                new ReadLacCompletion(readLacOpLogger, cb, ctx, ledgerId,
                        scheduleTimeout(completionKey, readEntryTimeout)));
        final Channel c = channel;
        if (c == null) {
            errorOutReadLacKey(completionKey);
            return;
        }

        try {
            ChannelFuture future = c.writeAndFlush(readLacRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Succssfully wrote request {} to {}", readLacRequest, c.remoteAddress());
                        }
                    } else {
                        if (!(future.cause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing readLac(lid = {}) to channel {} failed : ",
                                    new Object[] { ledgerId, c, future.cause() });
                        }
                        errorOutReadLacKey(completionKey);
                    }
                }
            });
        } catch(Throwable e) {
            LOG.warn("Read LAC operation {} failed", readLacRequest, e);
            errorOutReadLacKey(completionKey);
        }
    }

    public void readEntry(final long ledgerId, final long entryId, ReadEntryCallback cb, Object ctx) {
        Object request = null;
        CompletionKey completion = null;
        if (useV2WireProtocol) {
            request = new BookieProtocol.ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                    ledgerId, entryId, (short) 0);
            completion = new V2CompletionKey(ledgerId, entryId, OperationType.READ_ENTRY);
        } else {
            final long txnId = getTxnId();
            completion = new V3CompletionKey(txnId, OperationType.READ_ENTRY);

            // Build the request and calculate the total size to be included in the packet.
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.READ_ENTRY)
                    .setTxnId(txnId);

            ReadRequest.Builder readBuilder = ReadRequest.newBuilder()
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId);

            request = Request.newBuilder()
                    .setHeader(headerBuilder)
                    .setReadRequest(readBuilder)
                    .build();
        }
        final Object readRequest = request;
        final CompletionKey completionKey = completion;

        completionObjects.put(completionKey,
                new ReadCompletion(this, readEntryOpLogger, cb, ctx, ledgerId, entryId,
                        scheduleTimeout(completionKey, readEntryTimeout)));
        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(completionKey);
            return;
        }

        try{
            ChannelFuture future = c.writeAndFlush(readRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request {} to {}",
                                      readRequest, c.remoteAddress());
                        }
                    } else {
                        if (!(future.cause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing readEntry(lid={}, eid={}) to channel {} failed : ",
                                    new Object[] { ledgerId, entryId, c, future.cause() });
                        }
                        errorOutReadKey(completionKey);
                    }
                }
            });
        } catch(Throwable e) {
            LOG.warn("Read entry operation {} failed", readRequest, e);
            errorOutReadKey(completionKey);
        }
    }

    public void getBookieInfo(final long requested, GetBookieInfoCallback cb, Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new V3CompletionKey(txnId, OperationType.GET_BOOKIE_INFO);
        completionObjects.put(completionKey,
                new GetBookieInfoCompletion(this, getBookieInfoOpLogger, cb, ctx,
                                   scheduleTimeout(completionKey, getBookieInfoTimeout)));

        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.GET_BOOKIE_INFO)
                .setTxnId(txnId);

        GetBookieInfoRequest.Builder getBookieInfoBuilder = GetBookieInfoRequest.newBuilder()
                .setRequested(requested);

        final Request getBookieInfoRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setGetBookieInfoRequest(getBookieInfoBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(completionKey);
            return;
        }

        try{
            ChannelFuture future = c.writeAndFlush(getBookieInfoRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request {} to {}",
                                    getBookieInfoRequest, c.remoteAddress());
                        }
                    } else {
                        if (!(future.cause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing GetBookieInfoRequest(flags={}) to channel {} failed : ",
                                    new Object[] { requested, c, future.cause() });
                        }
                        errorOutReadKey(completionKey);
                    }
                }
            });
        } catch(Throwable e) {
            LOG.warn("Get metadata operation {} failed", getBookieInfoRequest, e);
            errorOutReadKey(completionKey);
        }
    }

    /**
     * Disconnects the bookie client. It can be reused.
     */
    public void disconnect() {
        disconnect(true);
    }

    public void disconnect(boolean wait) {
        LOG.info("Disconnecting the per channel bookie client for {}", addr);
        closeInternal(false, wait);
    }

    /**
     * Closes the bookie client permanently. It cannot be reused.
     */
    public void close() {
        close(true);
    }

    public void close(boolean wait) {
        LOG.info("Closing the per channel bookie client for {}", addr);
        closeLock.writeLock().lock();
        try {
            if (ConnectionState.CLOSED == state) {
                return;
            }
            state = ConnectionState.CLOSED;
            errorOutOutstandingEntries(BKException.Code.ClientClosedException);
        } finally {
            closeLock.writeLock().unlock();
        }
        closeInternal(true, wait);
    }

    private void closeInternal(boolean permanent, boolean wait) {
        Channel toClose = null;
        synchronized (this) {
            if (permanent) {
                state = ConnectionState.CLOSED;
            } else if (state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
            }
            toClose = channel;
            channel = null;
        }
        if (toClose != null) {
            ChannelFuture cf = closeChannel(toClose);
            if (wait) {
                cf.awaitUninterruptibly();
            }
        }
    }

    private ChannelFuture closeChannel(Channel c) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing channel {}", c);
        }
        return c.close();
    }

    void errorOutReadKey(final CompletionKey key) {
        errorOutReadKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutReadKey(final CompletionKey key, final int rc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing completion key: {}", key);
        }
        final ReadCompletion readCompletion = (ReadCompletion)completionObjects.remove(key);
        if (null == readCompletion) {
            return;
        }
        executor.submitOrdered(readCompletion.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if (c != null && c.remoteAddress() != null) {
                    bAddress = c.remoteAddress().toString();
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Could not write request for reading entry: {} ledger-id: {} bookie: {} rc: {}",
                            new Object[] { readCompletion.entryId, readCompletion.ledgerId, bAddress, rc });
                }

                readCompletion.cb.readEntryComplete(rc, readCompletion.ledgerId, readCompletion.entryId,
                                                    null, readCompletion.ctx);
            }

            @Override
            public String toString() {
                return String.format("ErrorOutReadKey(%s)", key);
            }
        });
    }

    void errorOutWriteLacKey(final CompletionKey key) {
        errorOutWriteLacKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutWriteLacKey(final CompletionKey key, final int rc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing completion key: {}", key);
        }
        final WriteLacCompletion writeLacCompletion = (WriteLacCompletion)completionObjects.remove(key);
        if (null == writeLacCompletion) {
            return;
        }
        executor.submitOrdered(writeLacCompletion.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if (c != null) {
                    bAddress = c.remoteAddress().toString();
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Could not write request writeLac for ledgerId: {} bookie: {}",
                            writeLacCompletion.ledgerId, bAddress);
                }
                writeLacCompletion.cb.writeLacComplete(rc, writeLacCompletion.ledgerId, addr, writeLacCompletion.ctx);
            }
        });
    }

    void errorOutReadLacKey(final CompletionKey key) {
        errorOutReadLacKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutReadLacKey(final CompletionKey key, final int rc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing completion key: {}", key);
        }
        final ReadLacCompletion readLacCompletion = (ReadLacCompletion)completionObjects.remove(key);
        if (null == readLacCompletion) {
            return;
        }
        executor.submitOrdered(readLacCompletion.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if (c != null) {
                    bAddress = c.remoteAddress().toString();
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Could not write request readLac for ledgerId: {} bookie: {}", readLacCompletion.ledgerId,
                            bAddress);
                }
                readLacCompletion.cb.readLacComplete(rc, readLacCompletion.ledgerId, null, null, readLacCompletion.ctx);
            }
        });
    }

    void errorOutAddKey(final CompletionKey key) {
        errorOutAddKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutAddKey(final CompletionKey key, final int rc) {
        final AddCompletion addCompletion = (AddCompletion)completionObjects.remove(key);
        if (null == addCompletion) {
            return;
        }
        executor.submitOrdered(addCompletion.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if (c != null && c.remoteAddress() != null) {
                    bAddress = c.remoteAddress().toString();
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Could not write request for adding entry: {} ledger-id: {} bookie: {} rc: {}",
                            new Object[] { addCompletion.entryId, addCompletion.ledgerId, bAddress, rc });
                }

                addCompletion.cb.writeComplete(rc, addCompletion.ledgerId, addCompletion.entryId,
                                               addr, addCompletion.ctx);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Invoked callback method: {}", addCompletion.entryId);
                }
            }

            @Override
            public String toString() {
                return String.format("ErrorOutAddKey(%s)", key);
            }
        });
    }

    void errorOutGetBookieInfoKey(final CompletionKey key) {
        errorOutGetBookieInfoKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutGetBookieInfoKey(final CompletionKey key, final int rc) {
        final GetBookieInfoCompletion getBookieInfoCompletion = (GetBookieInfoCompletion)completionObjects.remove(key);
        if (null == getBookieInfoCompletion) {
            return;
        }
        executor.submit(new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if (c != null) {
                    bAddress = c.remoteAddress().toString();
                }
                LOG.debug("Could not write getBookieInfo request for bookie: {}", new Object[] {bAddress});
                getBookieInfoCompletion.cb.getBookieInfoComplete(rc, new BookieInfo(), getBookieInfoCompletion.ctx);
            }
        });
    }

    /**
     * Errors out pending entries. We call this method from one thread to avoid
     * concurrent executions to QuorumOpMonitor (implements callbacks). It seems
     * simpler to call it from BookieHandle instead of calling directly from
     * here.
     */

    void errorOutOutstandingEntries(int rc) {

        // DO NOT rewrite these using Map.Entry iterations. We want to iterate
        // on keys and see if we are successfully able to remove the key from
        // the map. Because the add and the read methods also do the same thing
        // in case they get a write failure on the socket. The one who
        // successfully removes the key from the map is the one responsible for
        // calling the application callback.
        for (CompletionKey key : completionObjects.keySet()) {
            switch (key.operationType) {
                case ADD_ENTRY:
                    errorOutAddKey(key, rc);
                    break;
                case READ_ENTRY:
                    errorOutReadKey(key, rc);
                    break;
                default:
                    break;
            }
        }
    }

    void recordError() {
        if (pcbcPool != null) {
            pcbcPool.recordError();
        }
    }

    /**
     * If our channel has disconnected, we just error out the pending entries
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("Disconnected from bookie channel {}", ctx.channel());
        if (ctx.channel() != null) {
            closeChannel(ctx.channel());
        }

        errorOutOutstandingEntries(BKException.Code.BookieHandleNotAvailableException);

        synchronized (this) {
            if (this.channel == ctx.channel()
                && state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
            }
        }

        // we don't want to reconnect right away. If someone sends a request to
        // this address, we will reconnect.
    }

    /**
     * Called by netty when an exception happens in one of the netty threads
     * (mostly due to what we do in the netty threads)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof CorruptedFrameException || cause instanceof TooLongFrameException) {
            LOG.error("Corrupted frame received from bookie: {}", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        if (cause instanceof AuthHandler.AuthenticationException) {
            LOG.error("Error authenticating connection", cause);
            errorOutOutstandingEntries(BKException.Code.UnauthorizedAccessException);
            Channel c = ctx.channel();
            if (c != null) {
                closeChannel(c);
            }
            return;
        }

        if (cause instanceof IOException) {
            // these are thrown when a bookie fails, logging them just pollutes
            // the logs (the failure is logged from the listeners on the write
            // operation), so I'll just ignore it here.
            ctx.close();
            return;
        }

        synchronized (this) {
            if (state == ConnectionState.CLOSED) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Unexpected exception caught by bookie client channel handler, "
                            + "but the client is closed, so it isn't important", cause);
                }
            } else {
                LOG.error("Unexpected exception caught by bookie client channel handler", cause);
            }
        }

        // Since we are a library, cant terminate App here, can we?
        ctx.close();
    }

    /**
     * Called by netty when a message is received on a channel
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (msg instanceof BookieProtocol.Response) {
            BookieProtocol.Response response = (BookieProtocol.Response) msg;
            readV2Response(response);
        } else if (msg instanceof Response) {
            Response response = (Response) msg;
            readV3Response(response);
        } else {
        	ctx.fireChannelRead(msg);
        }
    }

    private void readV2Response(final BookieProtocol.Response response) {
        final long ledgerId = response.ledgerId;
        final long entryId = response.entryId;

        final OperationType operationType = getOperationType(response.getOpCode());
        final StatusCode status = getStatusCodeFromErrorCode(response.errorCode);

        final CompletionValue completionValue = completionObjects.remove(new V2CompletionKey(ledgerId, entryId, operationType));

        if (null == completionValue) {
            // Unexpected response, so log it. The txnId should have been present.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected response received from bookie : " + addr + " for type : " + operationType
                        + " and ledger:entry : " + ledgerId + ":" + entryId);
            }
        } else {
            long orderingKey = completionValue.ledgerId;

            executor.submitOrdered(orderingKey, new SafeRunnable() {
                @Override
                public void safeRun() {
                    switch (operationType) {
                        case ADD_ENTRY: {
                            handleAddResponse(ledgerId, entryId, status, completionValue);
                            break;
                        }
                        case READ_ENTRY: {
                            BookieProtocol.ReadResponse readResponse = (BookieProtocol.ReadResponse) response;
                            ByteBuf data = null;
                            if (readResponse.hasData()) {
                              data = readResponse.getData();
                            }
                            handleReadResponse(ledgerId, entryId, status, data, INVALID_ENTRY_ID, completionValue);
                            break;
                        }
                        default:
                            LOG.error("Unexpected response, type:{} received from bookie:{}, ignoring", operationType, addr);
                            break;
                    }
                }
            });
        }
    }

    private StatusCode getStatusCodeFromErrorCode(int errorCode) {
        switch (errorCode) {
            case BookieProtocol.EOK:
                return StatusCode.EOK;
            case BookieProtocol.ENOLEDGER:
                return StatusCode.ENOLEDGER;
            case BookieProtocol.ENOENTRY:
                return StatusCode.ENOENTRY;
            case BookieProtocol.EBADREQ:
                return StatusCode.EBADREQ;
            case BookieProtocol.EIO:
                return StatusCode.EIO;
            case BookieProtocol.EUA:
                return StatusCode.EUA;
            case BookieProtocol.EBADVERSION:
                return StatusCode.EBADVERSION;
            case BookieProtocol.EFENCED:
                return StatusCode.EFENCED;
            case BookieProtocol.EREADONLY:
                return StatusCode.EREADONLY;
            default:
                throw new IllegalArgumentException("Invalid error code: " + errorCode);
        }
    }

    private OperationType getOperationType(byte opCode) {
        switch (opCode) {
            case BookieProtocol.ADDENTRY:
                return  OperationType.ADD_ENTRY;
            case BookieProtocol.READENTRY:
                return OperationType.READ_ENTRY;
            case BookieProtocol.AUTH:
                return OperationType.AUTH;
            case BookieProtocol.READ_LAC:
                return OperationType.READ_LAC;
            case BookieProtocol.WRITE_LAC:
                return OperationType.WRITE_LAC;
            case BookieProtocol.GET_BOOKIE_INFO:
                return OperationType.GET_BOOKIE_INFO;
            default:
                throw new IllegalArgumentException("Invalid operation type");
        }
    }

    private void readV3Response(final Response response) {
        final BKPacketHeader header = response.getHeader();

        final CompletionValue completionValue = completionObjects.remove(newCompletionKey(header.getTxnId(),
                header.getOperation()));

        if (null == completionValue) {
            // Unexpected response, so log it. The txnId should have been present.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected response received from bookie : " + addr + " for type : " + header.             getOperation() +
                        " and txnId : " + header.getTxnId());
            }
        } else {
            long orderingKey = completionValue.ledgerId;
            executor.submitOrdered(orderingKey, new SafeRunnable() {
                @Override
                public void safeRun() {
                    OperationType type = header.getOperation();
                    switch (type) {
                        case ADD_ENTRY: {
                            AddResponse addResponse = response.getAddResponse();
                            StatusCode status = response.getStatus() == StatusCode.EOK ? addResponse.getStatus() : response.getStatus();
                            handleAddResponse(addResponse.getLedgerId(), addResponse.getEntryId(), status, completionValue);
                            break;
                        }
                        case READ_ENTRY: {
                            ReadResponse readResponse = response.getReadResponse();
                            StatusCode status = response.getStatus() == StatusCode.EOK ? readResponse.getStatus() : response.getStatus();
                            ByteBuf buffer = Unpooled.EMPTY_BUFFER;
                            if (readResponse.hasBody()) {
                                buffer = Unpooled.wrappedBuffer(readResponse.getBody().asReadOnlyByteBuffer());
                            }
                            long maxLAC = INVALID_ENTRY_ID;
                            if (readResponse.hasMaxLAC()) {
                                maxLAC = readResponse.getMaxLAC();
                            }
                            handleReadResponse(readResponse.getLedgerId(), readResponse.getEntryId(), status, buffer, maxLAC, completionValue);
                            break;
                        }
                        case WRITE_LAC: {
                            WriteLacResponse writeLacResponse = response.getWriteLacResponse();
                            StatusCode status = response.getStatus() == StatusCode.EOK ? writeLacResponse.getStatus() : response.getStatus();
                            handleWriteLacResponse(writeLacResponse.getLedgerId(), status, completionValue);
                            break;
                        }
                        case READ_LAC: {
                            ReadLacResponse readLacResponse = response.getReadLacResponse();
                            ByteBuf lacBuffer = Unpooled.EMPTY_BUFFER;
                            ByteBuf lastEntryBuffer = Unpooled.EMPTY_BUFFER;
                            StatusCode status = response.getStatus() == StatusCode.EOK ? readLacResponse.getStatus() : response.getStatus();
                            // Thread.dumpStack();

                            if (readLacResponse.hasLacBody()) {
                                lacBuffer = Unpooled.wrappedBuffer(readLacResponse.getLacBody().asReadOnlyByteBuffer());
                            }

                            if (readLacResponse.hasLastEntryBody()) {
                                lastEntryBuffer = Unpooled.wrappedBuffer(readLacResponse.getLastEntryBody().asReadOnlyByteBuffer());
                            }
                            handleReadLacResponse(readLacResponse.getLedgerId(), status, lacBuffer, lastEntryBuffer, completionValue);
                            break;
                        }
                        case GET_BOOKIE_INFO: {
                            GetBookieInfoResponse getBookieInfoResponse = response.getGetBookieInfoResponse();
                            StatusCode status = response.getStatus() == StatusCode.EOK ? getBookieInfoResponse.getStatus() : response.getStatus();
                            handleGetBookieInfoResponse(getBookieInfoResponse.getFreeDiskSpace(), getBookieInfoResponse.getTotalDiskCapacity(), status, completionValue);
                            break;
                        }
                        default:
                            LOG.error("Unexpected response, type:{} received from bookie:{}, ignoring",
                                      type, addr);
                            break;
                    }
                }

                @Override
                public String toString() {
                    return String.format("HandleResponse(Txn=%d, Type=%s, Entry=(%d, %d))",
                                         header.getTxnId(), header.getOperation(),
                                         completionValue.ledgerId, completionValue.entryId);
                }
            });
        }
    }

    void handleWriteLacResponse(long ledgerId, StatusCode status, CompletionValue completionValue) {
        // The completion value should always be an instance of an WriteLacCompletion object when we reach here.
        WriteLacCompletion plc = (WriteLacCompletion)completionValue;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for writeLac request from bookie: " + addr + " for ledger: " + ledgerId + " rc: "
                    + status);
        }

        // convert to BKException code
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.error("writeLac for ledger: " + ledgerId + " failed on bookie: " + addr
                        + " with code:" + status);
            rcToRet = BKException.Code.WriteException;
        }
        plc.cb.writeLacComplete(rcToRet, ledgerId, addr, plc.ctx);
    }

 void handleAddResponse(long ledgerId, long entryId, StatusCode status, CompletionValue completionValue) {
        // The completion value should always be an instance of an AddCompletion object when we reach here.
        AddCompletion ac = (AddCompletion)completionValue;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for add request from bookie: " + addr + " for ledger: " + ledgerId + " entry: "
                    + entryId + " rc: " + status);
        }
        // convert to BKException code because thats what the upper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Add for ledger: " + ledgerId + ", entry: " + entryId + " failed on bookie: " + addr
                        + " with code:" + status);
            }
            rcToRet = BKException.Code.WriteException;
        }
        ac.cb.writeComplete(rcToRet, ledgerId, entryId, addr, ac.ctx);
    }

    void handleReadLacResponse(long ledgerId, StatusCode status, ByteBuf lacBuffer, ByteBuf lastEntryBuffer, CompletionValue completionValue) {
        // The completion value should always be an instance of an WriteLacCompletion object when we reach here.
        ReadLacCompletion glac = (ReadLacCompletion)completionValue;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for readLac request from bookie: " + addr + " for ledger: " + ledgerId + " rc: "
                    + status);
        }
        // convert to BKException code
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("readLac for ledger: " + ledgerId + " failed on bookie: " + addr + " with code:" + status);
            }
            rcToRet = BKException.Code.ReadException;
        }
        glac.cb.readLacComplete(rcToRet, ledgerId, lacBuffer.slice(), lastEntryBuffer.slice(), glac.ctx);
    }

    void handleReadResponse(long ledgerId,
                            long entryId,
                            StatusCode status,
                            ByteBuf buffer,
                            long maxLAC, // max known lac piggy-back from bookies
                            CompletionValue completionValue) {
        // The completion value should always be an instance of a ReadCompletion object when we reach here.
        ReadCompletion rc = (ReadCompletion)completionValue;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for read request from bookie: " + addr + " for ledger: " + ledgerId + " entry: "
                    + entryId + " rc: " + rc + " entry length: " + buffer.readableBytes());
        }

        // convert to BKException code because thats what the uppper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.error("Read entry for ledger:{}, entry:{} failed on bookie:{} with code:{}",
                      new Object[] { ledgerId, entryId, addr, status });
            rcToRet = BKException.Code.ReadException;
        }
        if(buffer != null) {
            buffer = buffer.slice();
        }
        if (maxLAC > INVALID_ENTRY_ID && (rc.ctx instanceof ReadEntryCallbackCtx)) {
            ((ReadEntryCallbackCtx) rc.ctx).setLastAddConfirmed(maxLAC);
        }
        rc.cb.readEntryComplete(rcToRet, ledgerId, entryId, buffer, rc.ctx);
    }

    void handleGetBookieInfoResponse(long freeDiskSpace, long totalDiskCapacity,  StatusCode status, CompletionValue completionValue) {
        // The completion value should always be an instance of a GetBookieInfoCompletion object when we reach here.
        GetBookieInfoCompletion rc = (GetBookieInfoCompletion)completionValue;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for read metadata request from bookie: {} rc {}", addr, rc);
        }

        // convert to BKException code because thats what the upper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.error("Read metadata failed on bookie:{} with code:{}",
                      new Object[] { addr, status });
            rcToRet = BKException.Code.ReadException;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Response received from bookie info read: freeDiskSpace=" + freeDiskSpace + " totalDiskSpace:"
                    + totalDiskCapacity);
        }
        rc.cb.getBookieInfoComplete(rcToRet, new BookieInfo(totalDiskCapacity, freeDiskSpace), rc.ctx);
    }

    /**
     * Boiler-plate wrapper classes follow
     *
     */

    // visible for testing
    static abstract class CompletionValue {
        final Object ctx;
        protected final long ledgerId;
        protected final long entryId;
        protected final Timeout timeout;

        public CompletionValue(Object ctx, long ledgerId, long entryId,
                               Timeout timeout) {
            this.ctx = ctx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.timeout = timeout;
        }

        void cancelTimeout() {
            if (null != timeout) {
                timeout.cancel();
            }
        }
    }

    // visible for testing
    static class WriteLacCompletion extends CompletionValue {
        final WriteLacCallback cb;

        public WriteLacCompletion(WriteLacCallback cb, Object ctx, long ledgerId) {
            this(null, cb, ctx, ledgerId, null);
        }

        public WriteLacCompletion(final OpStatsLogger writeLacOpLogger, final WriteLacCallback originalCallback,
                final Object originalCtx, final long ledgerId, final Timeout timeout) {
            super(originalCtx, ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, timeout);
            final long startTime = MathUtils.nowInNano();
            this.cb = null == writeLacOpLogger ? originalCallback : new WriteLacCallback() {
                @Override
                public void writeLacComplete(int rc, long ledgerId, BookieSocketAddress addr, Object ctx) {
                    cancelTimeout();
                    long latency = MathUtils.elapsedNanos(startTime);
                    if (rc != BKException.Code.OK) {
                        writeLacOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                    } else {
                        writeLacOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                    }
                    originalCallback.writeLacComplete(rc, ledgerId, addr, originalCtx);
                }
            };

        }
    }

    // visible for testing
    static class ReadLacCompletion extends CompletionValue {
        final ReadLacCallback cb;

        public ReadLacCompletion(ReadLacCallback cb, Object ctx, long ledgerId) {
            this (null, cb, ctx, ledgerId, null);
        }

        public ReadLacCompletion(final OpStatsLogger readLacOpLogger, final ReadLacCallback originalCallback,
                final Object ctx, final long ledgerId, final Timeout timeout) {
            super(ctx, ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, timeout);
            final long startTime = MathUtils.nowInNano();
            this.cb = null == readLacOpLogger ? originalCallback : new ReadLacCallback() {
                @Override
                public void readLacComplete(int rc, long ledgerId, ByteBuf lacBuffer, ByteBuf lastEntryBuffer,
                        Object ctx) {
                    cancelTimeout();
                    long latency = MathUtils.elapsedNanos(startTime);
                    if (rc != BKException.Code.OK) {
                        readLacOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                    } else {
                        readLacOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                    }
                    originalCallback.readLacComplete(rc, ledgerId, lacBuffer, lastEntryBuffer, ctx);
                }
            };
        }
    }

    // visible for testing
    static class ReadCompletion extends CompletionValue {
        final ReadEntryCallback cb;

        public ReadCompletion(final PerChannelBookieClient pcbc, ReadEntryCallback cb, Object ctx,
                              long ledgerId, long entryId) {
            this(pcbc, null, cb, ctx, ledgerId, entryId, null);
        }

        public ReadCompletion(final PerChannelBookieClient pcbc, final OpStatsLogger readEntryOpLogger,
                              final ReadEntryCallback originalCallback,
                              final Object originalCtx, final long ledgerId, final long entryId,
                              final Timeout timeout) {
            super(originalCtx, ledgerId, entryId, timeout);
            final long startTime = MathUtils.nowInNano();
            this.cb = new ReadEntryCallback() {
                @Override
                public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
                    cancelTimeout();
                    if (readEntryOpLogger != null) {
                        long latency = MathUtils.elapsedNanos(startTime);
                        if (rc != BKException.Code.OK) {
                            readEntryOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                        } else {
                            readEntryOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                        }
                    }

                    if (rc != BKException.Code.OK && !expectedBkOperationErrors.contains(rc)) {
                        pcbc.recordError();
                    }

                    originalCallback.readEntryComplete(rc, ledgerId, entryId, buffer, originalCtx);
                }
            };
        }
    }

    // visible for testing
    static class GetBookieInfoCompletion extends CompletionValue {
        final GetBookieInfoCallback cb;

        public GetBookieInfoCompletion(final PerChannelBookieClient pcbc, GetBookieInfoCallback cb, Object ctx) {
            this(pcbc, null, cb, ctx, null);
        }

        public GetBookieInfoCompletion(final PerChannelBookieClient pcbc, final OpStatsLogger getBookieInfoOpLogger,
                              final GetBookieInfoCallback originalCallback,
                              final Object originalCtx, final Timeout timeout) {
            super(originalCtx, 0L, 0L, timeout);
            final long startTime = MathUtils.nowInNano();
            this.cb = (null == getBookieInfoOpLogger) ? originalCallback : new GetBookieInfoCallback() {
                @Override
                public void getBookieInfoComplete(int rc, BookieInfo bInfo, Object ctx) {
                    cancelTimeout();
                    if (getBookieInfoOpLogger != null) {
                        long latency = MathUtils.elapsedNanos(startTime);
                        if (rc != BKException.Code.OK) {
                            getBookieInfoOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                        } else {
                            getBookieInfoOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                        }
                    }

                    if (rc != BKException.Code.OK && !expectedBkOperationErrors.contains(rc)) {
                        pcbc.recordError();
                    }

                    originalCallback.getBookieInfoComplete(rc, bInfo, originalCtx);
                }
            };
        }
    }

    // visible for testing
    static class AddCompletion extends CompletionValue {
        final WriteCallback cb;

        public AddCompletion(final PerChannelBookieClient pcbc, WriteCallback cb, Object ctx,
                             long ledgerId, long entryId) {
            this(pcbc, null, cb, ctx, ledgerId, entryId, null);
        }

        public AddCompletion(final PerChannelBookieClient pcbc, final OpStatsLogger addEntryOpLogger,
                             final WriteCallback originalCallback,
                             final Object originalCtx, final long ledgerId, final long entryId,
                             final Timeout timeout) {
            super(originalCtx, ledgerId, entryId, timeout);
            final long startTime = MathUtils.nowInNano();
            this.cb = null == addEntryOpLogger ? originalCallback : new WriteCallback() {
                @Override
                public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                    cancelTimeout();
                    if (pcbc.addEntryOpLogger != null) {
                        long latency = MathUtils.elapsedNanos(startTime);
                        if (rc != BKException.Code.OK) {
                            pcbc.addEntryOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                        } else {
                            pcbc.addEntryOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                        }
                    }

                    if (rc != BKException.Code.OK && !expectedBkOperationErrors.contains(rc)) {
                        pcbc.recordError();
                    }

                    originalCallback.writeComplete(rc, ledgerId, entryId, addr, originalCtx);
                }
            };
        }
    }

    // visable for testing
    CompletionKey newCompletionKey(long txnId, OperationType operationType) {
        return new V3CompletionKey(txnId, operationType);
    }

    Timeout scheduleTimeout(CompletionKey key, long timeout) {
        if (null != requestTimer) {
            return requestTimer.newTimeout(key, timeout, TimeUnit.SECONDS);
        } else {
            return null;
        }
    }

    class V3CompletionKey extends CompletionKey {

        public V3CompletionKey(long txnId, OperationType operationType) {
            super(txnId, operationType);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof V3CompletionKey)) {
                return false;
            }
            V3CompletionKey that = (V3CompletionKey) obj;
            return this.txnId == that.txnId && this.operationType == that.operationType;
        }

        @Override
        public int hashCode() {
            return ((int) txnId);
        }

        @Override
        public String toString() {
            return String.format("TxnId(%d), OperationType(%s)", txnId, operationType);
        }

    }

    abstract class CompletionKey implements TimerTask {
        final long txnId;
        final OperationType operationType;
        final long requestAt;

        CompletionKey(long txnId, OperationType operationType) {
            this.txnId = txnId;
            this.operationType = operationType;
            this.requestAt = MathUtils.nowInNano();
        }

        private long elapsedTime() {
            return MathUtils.elapsedNanos(requestAt);
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }
            if (OperationType.ADD_ENTRY == operationType) {
                errorOutAddKey(this, BKException.Code.TimeoutException);
                addTimeoutOpLogger.registerSuccessfulEvent(elapsedTime(), TimeUnit.NANOSECONDS);
            } else if (OperationType.READ_ENTRY == operationType) {
                errorOutReadKey(this, BKException.Code.TimeoutException);
                readTimeoutOpLogger.registerSuccessfulEvent(elapsedTime(), TimeUnit.NANOSECONDS);
            } else if (OperationType.WRITE_LAC == operationType) {
                errorOutWriteLacKey(this, BKException.Code.TimeoutException);
                writeLacTimeoutOpLogger.registerSuccessfulEvent(elapsedTime(), TimeUnit.NANOSECONDS);
            } else if (OperationType.READ_LAC == operationType) {
                errorOutReadLacKey(this, BKException.Code.TimeoutException);
                readLacTimeoutOpLogger.registerSuccessfulEvent(elapsedTime(), TimeUnit.NANOSECONDS);
            } else {
                errorOutGetBookieInfoKey(this, BKException.Code.TimeoutException);
                getBookieInfoTimeoutOpLogger.registerSuccessfulEvent(elapsedTime(), TimeUnit.NANOSECONDS);
            }
	}
    }


    /**
     * Note : Helper functions follow
     */

    /**
     * @param status
     * @return null if the statuscode is unknown.
     */
    private Integer statusCodeToExceptionCode(StatusCode status) {
        Integer rcToRet = null;
        switch (status) {
            case EOK:
                rcToRet = BKException.Code.OK;
                break;
            case ENOENTRY:
                rcToRet = BKException.Code.NoSuchEntryException;
                break;
            case ENOLEDGER:
                rcToRet = BKException.Code.NoSuchLedgerExistsException;
                break;
            case EBADVERSION:
                rcToRet = BKException.Code.ProtocolVersionException;
                break;
            case EUA:
                rcToRet = BKException.Code.UnauthorizedAccessException;
                break;
            case EFENCED:
                rcToRet = BKException.Code.LedgerFencedException;
                break;
            case EREADONLY:
                rcToRet = BKException.Code.WriteOnReadOnlyBookieException;
                break;
            default:
                break;
        }
        return rcToRet;
    }

    private long getTxnId() {
        return txnIdGenerator.incrementAndGet();
    }

    private class V2CompletionKey extends CompletionKey {
        final long ledgerId;
        final long entryId;


        public V2CompletionKey(long ledgerId, long entryId, OperationType operationType) {
            super(0, operationType);
            this.ledgerId = ledgerId;
            this.entryId = entryId;

        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof V2CompletionKey)) {
                return  false;
            }
            V2CompletionKey that = (V2CompletionKey) object;
            return  this.entryId == that.entryId && this.ledgerId == that.ledgerId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ledgerId, entryId);
        }

        @Override
        public String toString() {
            return String.format("%d:%d %s", ledgerId, entryId, operationType);
        }
    }
}
