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

import com.google.common.collect.Sets;
import com.google.common.base.Joiner;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
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
import io.netty.handler.ssl.SslHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Collection;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.auth.BookKeeperPrincipal;
import org.apache.bookkeeper.auth.ClientAuthProvider;
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
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.StartTLSCallback;
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
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.tls.SecurityHandlerFactory.NodeType;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.protobuf.ExtensionRegistry;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.net.SocketAddress;

import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.bookkeeper.auth.BookKeeperPrincipal;

/**
 * This class manages all details of connection to a particular bookie. It also
 * has reconnect logic if a connection to a bookie fails.
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
    final int startTLSTimeout;

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
    private final OpStatsLogger startTLSOpLogger;
    private final OpStatsLogger startTLSTimeoutOpLogger;

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
        DISCONNECTED, CONNECTING, CONNECTED, CLOSED, START_TLS
    }

    volatile ConnectionState state;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
    private final ClientConfiguration conf;

    private final PerChannelBookieClientPool pcbcPool;
    private final ClientAuthProvider.Factory authProviderFactory;
    private final ExtensionRegistry extRegistry;
    private final SecurityHandlerFactory shFactory;

    public PerChannelBookieClient(OrderedSafeExecutor executor, EventLoopGroup eventLoopGroup,
                                  BookieSocketAddress addr) throws SecurityException {
        this(new ClientConfiguration(), executor, eventLoopGroup, addr, null, NullStatsLogger.INSTANCE, null, null,
                null);
    }

    public PerChannelBookieClient(OrderedSafeExecutor executor, EventLoopGroup eventLoopGroup,
                                  BookieSocketAddress addr,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry) throws SecurityException {
        this(new ClientConfiguration(), executor, eventLoopGroup, addr, null, NullStatsLogger.INSTANCE,
                authProviderFactory, extRegistry, null);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedSafeExecutor executor,
                                  EventLoopGroup eventLoopGroup, BookieSocketAddress addr,
                                  HashedWheelTimer requestTimer, StatsLogger parentStatsLogger,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry,
                                  PerChannelBookieClientPool pcbcPool) throws SecurityException {
       this(conf, executor, eventLoopGroup, addr, null, NullStatsLogger.INSTANCE,
                authProviderFactory, extRegistry, pcbcPool, null);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedSafeExecutor executor,
                                  EventLoopGroup eventLoopGroup, BookieSocketAddress addr,
                                  HashedWheelTimer requestTimer, StatsLogger parentStatsLogger,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry,
                                  PerChannelBookieClientPool pcbcPool,
                                  SecurityHandlerFactory shFactory) throws SecurityException {
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
        this.startTLSTimeout = conf.getStartTLSTimeout();
        this.useV2WireProtocol = conf.getUseV2WireProtocol();

        this.authProviderFactory = authProviderFactory;
        this.extRegistry = extRegistry;
        this.shFactory = shFactory;
        if (shFactory != null) {
            shFactory.init(NodeType.Client, conf);
        }

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
        startTLSOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_START_TLS_OP);
        startTLSTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_START_TLS_OP);

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
                Channel c = channel;
                if (c == null) {
                    return Collections.emptyList();
                }
                SslHandler ssl = c.pipeline().get(SslHandler.class);
                if (ssl == null) {
                    return Collections.emptyList();
                }
                try {
                    Certificate[] certificates = ssl.engine().getSession().getPeerCertificates();
                    if (certificates == null) {
                        return Collections.emptyList();
                    }
                    List<Object> result = new ArrayList<>();
                    result.addAll(Arrays.asList(certificates));
                    return result;
                } catch (SSLPeerUnverifiedException err) {
                     return Collections.emptyList();
                }
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

            @Override
            public boolean isSecure() {
               Channel c = channel;
               if (c == null) {
                    return false;
               } else {
                    return c.pipeline().get(SslHandler.class) != null;
               }
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

        ByteBufAllocator allocator;
        if (this.conf.isNettyUsePooledBuffers()) {
            allocator = PooledByteBufAllocator.DEFAULT;
        } else {
            allocator = UnpooledByteBufAllocator.DEFAULT;
        }

        bootstrap.option(ChannelOption.ALLOCATOR, allocator);
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
        future.addListener(new ConnectionFutureListener());
        return future;
    }

    void cleanDisconnectAndClose() {
        disconnect();
        close();
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

                    if (state == ConnectionState.CONNECTING
                        || state == ConnectionState.START_TLS) {
                        // the connection request has already been sent and it is waiting for the response.
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
        final CompletionKey completionKey = new V3CompletionKey(txnId,
                                                                OperationType.WRITE_LAC);
        // writeLac is mostly like addEntry hence uses addEntryTimeout
        completionObjects.put(completionKey,
                              new WriteLacCompletion(completionKey, cb,
                                                     ctx, lac));

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
        writeAndFlush(channel, completionKey, writeLacRequest);
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
        CompletionKey completionKey = null;
        if (useV2WireProtocol) {
            completionKey = acquireV2Key(ledgerId, entryId, OperationType.ADD_ENTRY);
            request = BookieProtocol.AddRequest.create(
                    BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId,
                    (short) options, masterKey, toSend);
        } else {
            final long txnId = getTxnId();
            completionKey = new V3CompletionKey(txnId, OperationType.ADD_ENTRY);

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

        completionObjects.put(completionKey,
                              acquireAddCompletion(completionKey,
                                                   cb, ctx, ledgerId, entryId));
        final Channel c = channel;
        if (c == null) {
            // usually checked in writeAndFlush, but we have extra check
            // because we need to release toSend.
            errorOut(completionKey);
            toSend.release();
            return;
        } else {
            writeAndFlush(c, completionKey, request);
        }
    }

    public void readEntryAndFenceLedger(final long ledgerId, byte[] masterKey,
                                        final long entryId,
                                        ReadEntryCallback cb, Object ctx) {
        Object request = null;
        CompletionKey completionKey = null;
        if (useV2WireProtocol) {
            completionKey = acquireV2Key(ledgerId, entryId, OperationType.READ_ENTRY);
            request = new BookieProtocol.ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId,
                    BookieProtocol.FLAG_DO_FENCING, masterKey);
        } else {
            final long txnId = getTxnId();
            completionKey = new V3CompletionKey(txnId, OperationType.READ_ENTRY);

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

        CompletionValue completion = new ReadCompletion(completionKey,
                                                        cb, ctx,
                                                        ledgerId, entryId);
        if (completionObjects.putIfAbsent(
                    completionKey, completion) != null) {
            // We cannot have more than 1 pending read on the same ledger/entry in the v2 protocol
            completion.errorOut(BKException.Code.BookieHandleNotAvailableException);
            return;
        }

        writeAndFlush(channel, completionKey, request);
    }

    public void readLac(final long ledgerId, ReadLacCallback cb, Object ctx) {
        Object request = null;
        CompletionKey completionKey = null;
        if (useV2WireProtocol) {
            request = new BookieProtocol.ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                    ledgerId, 0, (short) 0);
            completionKey = acquireV2Key(ledgerId, 0, OperationType.READ_LAC);
        } else {
            final long txnId = getTxnId();
            completionKey = new V3CompletionKey(txnId, OperationType.READ_LAC);

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
        completionObjects.put(completionKey,
                              new ReadLacCompletion(completionKey, cb,
                                                    ctx, ledgerId));
        writeAndFlush(channel, completionKey, request);
    }

    /**
     * Long Poll Reads
     */
    public void readEntryWaitForLACUpdate(final long ledgerId,
                                          final long entryId,
                                          final long previousLAC,
                                          final long timeOutInMillis,
                                          final boolean piggyBackEntry,
                                          ReadEntryCallback cb,
                                          Object ctx) {
        readEntryInternal(ledgerId, entryId, previousLAC, timeOutInMillis, piggyBackEntry, cb, ctx);
    }

    /**
     * Normal Reads.
     */
    public void readEntry(final long ledgerId,
                          final long entryId,
                          ReadEntryCallback cb,
                          Object ctx) {
        readEntryInternal(ledgerId, entryId, null, null, false, cb, ctx);
    }

    private void readEntryInternal(final long ledgerId,
                                   final long entryId,
                                   final Long previousLAC,
                                   final Long timeOutInMillis,
                                   final boolean piggyBackEntry,
                                   final ReadEntryCallback cb,
                                   final Object ctx) {
        Object request = null;
        CompletionKey completionKey = null;
        if (useV2WireProtocol) {
            request = new BookieProtocol.ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                    ledgerId, entryId, (short) 0);
            completionKey = acquireV2Key(ledgerId, entryId, OperationType.READ_ENTRY);
        } else {
            final long txnId = getTxnId();
            completionKey = new V3CompletionKey(txnId, OperationType.READ_ENTRY);

            // Build the request and calculate the total size to be included in the packet.
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.READ_ENTRY)
                    .setTxnId(txnId);

            ReadRequest.Builder readBuilder = ReadRequest.newBuilder()
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId);

            if (null != previousLAC) {
                readBuilder = readBuilder.setPreviousLAC(previousLAC);
            }

            if (null != timeOutInMillis) {
                // Long poll requires previousLAC
                if (null == previousLAC) {
                    cb.readEntryComplete(BKException.Code.IncorrectParameterException,
                        ledgerId, entryId, null, ctx);
                    return;
                }
                readBuilder = readBuilder.setTimeOut(timeOutInMillis);
            }

            if (piggyBackEntry) {
                // Long poll requires previousLAC
                if (null == previousLAC) {
                    cb.readEntryComplete(BKException.Code.IncorrectParameterException,
                        ledgerId, entryId, null, ctx);
                    return;
                }
                readBuilder = readBuilder.setFlag(ReadRequest.Flag.ENTRY_PIGGYBACK);
            }

            request = Request.newBuilder()
                    .setHeader(headerBuilder)
                    .setReadRequest(readBuilder)
                    .build();
        }

        CompletionValue completion = new ReadCompletion(completionKey, cb,
                                                        ctx, ledgerId, entryId);
        CompletionValue existingValue = completionObjects.putIfAbsent(
                completionKey, completion);
        if (existingValue != null) {
            // There's a pending read request on same ledger/entry. This is not supported in V2 protocol
            LOG.warn("Failing concurrent request to read at ledger: {} entry: {}", ledgerId, entryId);
            completion.errorOut(BKException.Code.UnexpectedConditionException);
            return;
        }

        writeAndFlush(channel, completionKey, request);
    }

    public void getBookieInfo(final long requested, GetBookieInfoCallback cb, Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new V3CompletionKey(txnId, OperationType.GET_BOOKIE_INFO);
        completionObjects.put(completionKey,
                              new GetBookieInfoCompletion(
                                      completionKey, cb, ctx));

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

        writeAndFlush(channel, completionKey, getBookieInfoRequest);
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

    private void writeAndFlush(final Channel channel,
                               final CompletionKey key,
                               final Object request) {
        if (channel == null) {
            errorOut(key);
            return;
        }

        try{
            channel.writeAndFlush(request, channel.voidPromise());
        } catch(Throwable e) {
            LOG.warn("Operation {} failed", requestToString(request), e);
            errorOut(key);
        }
    }

    private static String requestToString(Object request) {
        if (request instanceof BookkeeperProtocol.Request) {
            BookkeeperProtocol.BKPacketHeader header
                = ((BookkeeperProtocol.Request)request).getHeader();
            return String.format("Req(txnId=%d,op=%s,version=%s)",
                                 header.getTxnId(), header.getOperation(),
                                 header.getVersion());
        } else {
            return request.toString();
        }
    }

    void errorOut(final CompletionKey key) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing completion key: {}", key);
        }
        CompletionValue completion = completionObjects.remove(key);
        if (completion != null) {
            completion.errorOut();
        }
    }

    void errorOut(final CompletionKey key, final int rc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing completion key: {}", key);
        }
        CompletionValue completion = completionObjects.remove(key);
        if (completion != null) {
            completion.errorOut(rc);
        }
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
            errorOut(key, rc);
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

        final CompletionKey key = acquireV2Key(ledgerId, entryId, operationType);
        final CompletionValue completionValue = completionObjects.remove(key);
        key.release();

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
                        completionValue.handleV2Response(ledgerId, entryId,
                                                         status, response);
                        response.recycle();
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
            case BookieProtocol.ETOOMANYREQUESTS:
                return StatusCode.ETOOMANYREQUESTS;
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
                LOG.debug("Unexpected response received from bookie : " + addr + " for type : "
                        + header.getOperation() + " and txnId : " + header.getTxnId());
            }
        } else {
            long orderingKey = completionValue.ledgerId;
            executor.submitOrdered(orderingKey, new SafeRunnable() {
                @Override
                public void safeRun() {
                    completionValue.handleV3Response(response);
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

    void initTLSHandshake() {
        // create TLS handler
        PerChannelBookieClient parentObj = PerChannelBookieClient.this;
        SslHandler handler = parentObj.shFactory.newTLSHandler();
        channel.pipeline().addFirst(parentObj.shFactory.getHandlerName(), handler);
        handler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
                @Override
                public void operationComplete(Future<Channel> future) throws Exception {
                    int rc;
                    Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;

                    synchronized (PerChannelBookieClient.this) {
                        if (future.isSuccess() && state == ConnectionState.CONNECTING) {
                            LOG.error("Connection state changed before TLS handshake completed {}/{}", addr, state);
                            rc = BKException.Code.BookieHandleNotAvailableException;
                            closeChannel(future.get());
                            channel = null;
                            if (state != ConnectionState.CLOSED) {
                                state = ConnectionState.DISCONNECTED;
                            }
                        } else if (future.isSuccess() && state == ConnectionState.START_TLS) {
                            rc = BKException.Code.OK;
                            LOG.info("Successfully connected to bookie using TLS: " + addr);

                            state = ConnectionState.CONNECTED;
                            AuthHandler.ClientSideHandler authHandler = future.get().pipeline()
                                    .get(AuthHandler.ClientSideHandler.class);
                            authHandler.authProvider.onProtocolUpgrade();
                        } else if (future.isSuccess()
                                && (state == ConnectionState.CLOSED || state == ConnectionState.DISCONNECTED)) {
                            LOG.warn("Closed before TLS handshake completed, clean up: {}, current state {}",
                                    future.get(), state);
                            closeChannel(future.get());
                            rc = BKException.Code.BookieHandleNotAvailableException;
                            channel = null;
                        } else if (future.isSuccess() && state == ConnectionState.CONNECTED) {
                            LOG.debug("Already connected with another channel({}), so close the new channel({})",
                                    channel, future.get());
                            closeChannel(future.get());
                            return; // pendingOps should have been completed when other channel connected
                        } else {
                            LOG.error("TLS handshake failed with bookie: {}/{}, current state {} : ",
                                    new Object[] { future.get(), addr, state, future.cause() });
                            rc = BKException.Code.SecurityException;
                            closeChannel(future.get());
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
                        pendingOps = new ArrayDeque<>();
                    }

                    for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
                        pendingOp.operationComplete(rc, PerChannelBookieClient.this);
                    }
                }
            });
    }

    /**
     * Boiler-plate wrapper classes follow
     *
     */

    // visible for testing
    abstract class CompletionValue {
        private final OpStatsLogger opLogger;
        private final OpStatsLogger timeoutOpLogger;
        private final String operationName;
        protected Object ctx;
        protected long ledgerId;
        protected long entryId;
        protected long startTime;
        protected Timeout timeout;

        public CompletionValue(String operationName,
                               Object ctx,
                               long ledgerId, long entryId,
                               OpStatsLogger opLogger,
                               OpStatsLogger timeoutOpLogger,
                               Timeout timeout) {
            this.operationName = operationName;
            this.ctx = ctx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.startTime = MathUtils.nowInNano();
            this.opLogger = opLogger;
            this.timeoutOpLogger = timeoutOpLogger;
            this.timeout = timeout;
        }

        private long latency() {
            return MathUtils.elapsedNanos(startTime);
        }

        void cancelTimeoutAndLogOp(int rc) {
            Timeout t = timeout;
            if (null != t) {
                t.cancel();
            }

            if (rc != BKException.Code.OK) {
                opLogger.registerFailedEvent(latency(), TimeUnit.NANOSECONDS);
            } else {
                opLogger.registerSuccessfulEvent(latency(), TimeUnit.NANOSECONDS);
            }

            if (rc != BKException.Code.OK
                && !expectedBkOperationErrors.contains(rc)) {
                recordError();
            }
        }

        void timeout() {
            errorOut(BKException.Code.TimeoutException);
            timeoutOpLogger.registerSuccessfulEvent(latency(),
                                                    TimeUnit.NANOSECONDS);
        }

        protected int logAndConvertStatus(StatusCode status, int defaultStatus,
                                          Object... extraInfo) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got {} response from bookie:{} rc:{}, {}",
                          operationName, addr, status,
                          Joiner.on(":").join(extraInfo));
            }

            // convert to BKException code
            Integer rcToRet = statusCodeToExceptionCode(status);
            if (null == rcToRet) {
                LOG.error("{} for failed on bookie {} code {}",
                          operationName, addr, status);
                return defaultStatus;
            } else {
                return rcToRet;
            }
        }


        public abstract void errorOut();
        public abstract void errorOut(final int rc);

        protected void errorOutAndRunCallback(final Runnable callback) {
            executor.submitOrdered(ledgerId,
                    new SafeRunnable() {
                        @Override
                        public void safeRun() {
                            String bAddress = "null";
                            Channel c = channel;
                            if (c != null && c.remoteAddress() != null) {
                                bAddress = c.remoteAddress().toString();
                            }
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Could not write {} request to bookie {} for ledger {}, entry {}",
                                          operationName, bAddress,
                                          ledgerId, entryId);
                            }
                            callback.run();
                        }
                    });
        }

        public void handleV2Response(
                long ledgerId, long entryId, StatusCode status,
                BookieProtocol.Response response) {
            LOG.warn("Unhandled V2 response {}", response);
        }

        public abstract void handleV3Response(
                BookkeeperProtocol.Response response);
    }

    // visible for testing
    class WriteLacCompletion extends CompletionValue {
        final WriteLacCallback cb;

        public WriteLacCompletion(final CompletionKey key,
                                  final WriteLacCallback originalCallback,
                                  final Object originalCtx,
                                  final long ledgerId) {
            super("WriteLAC",
                  originalCtx, ledgerId, BookieProtocol.LAST_ADD_CONFIRMED,
                  writeLacOpLogger, writeLacTimeoutOpLogger,
                  scheduleTimeout(key, addEntryTimeout));
            this.cb = new WriteLacCallback() {
                    @Override
                    public void writeLacComplete(int rc, long ledgerId,
                                                 BookieSocketAddress addr,
                                                 Object ctx) {
                        cancelTimeoutAndLogOp(rc);
                        originalCallback.writeLacComplete(rc, ledgerId,
                                                          addr, originalCtx);
                        key.release();
                    }
                };
        }

        @Override
        public void errorOut() {
            errorOut(BKException.Code.BookieHandleNotAvailableException);
        }

        @Override
        public void errorOut(final int rc) {
            errorOutAndRunCallback(
                    () -> cb.writeLacComplete(rc, ledgerId, addr, ctx));
        }

        @Override
        public void handleV3Response(BookkeeperProtocol.Response response) {
            WriteLacResponse writeLacResponse = response.getWriteLacResponse();
            StatusCode status = response.getStatus() == StatusCode.EOK ?
                writeLacResponse.getStatus() : response.getStatus();
            long ledgerId = writeLacResponse.getLedgerId();

            int rc = logAndConvertStatus(status,
                                         BKException.Code.WriteException,
                                         "ledger", ledgerId);
            cb.writeLacComplete(rc, ledgerId, addr, ctx);
        }
    }

    // visible for testing
    class ReadLacCompletion extends CompletionValue {
        final ReadLacCallback cb;

        public ReadLacCompletion(final CompletionKey key,
                                 ReadLacCallback originalCallback,
                                 final Object ctx, final long ledgerId) {
            super("ReadLAC", ctx, ledgerId, BookieProtocol.LAST_ADD_CONFIRMED,
                  readLacOpLogger, readLacTimeoutOpLogger,
                  scheduleTimeout(key, readEntryTimeout));
            this.cb = new ReadLacCallback() {
                    @Override
                    public void readLacComplete(int rc, long ledgerId,
                                                ByteBuf lacBuffer,
                                                ByteBuf lastEntryBuffer,
                                                Object ctx) {
                        cancelTimeoutAndLogOp(rc);
                        originalCallback.readLacComplete(
                                rc, ledgerId, lacBuffer, lastEntryBuffer, ctx);
                        key.release();
                    }
                };
        }

        @Override
        public void errorOut() {
            errorOut(BKException.Code.BookieHandleNotAvailableException);
        }

        @Override
        public void errorOut(final int rc) {
            errorOutAndRunCallback(
                    () -> cb.readLacComplete(rc, ledgerId, null, null, ctx));
        }

        @Override
        public void handleV3Response(BookkeeperProtocol.Response response) {
            ReadLacResponse readLacResponse = response.getReadLacResponse();
            ByteBuf lacBuffer = Unpooled.EMPTY_BUFFER;
            ByteBuf lastEntryBuffer = Unpooled.EMPTY_BUFFER;
            StatusCode status = response.getStatus() == StatusCode.EOK ? readLacResponse.getStatus() : response.getStatus();

            if (readLacResponse.hasLacBody()) {
                lacBuffer = Unpooled.wrappedBuffer(readLacResponse.getLacBody().asReadOnlyByteBuffer());
            }

            if (readLacResponse.hasLastEntryBody()) {
                lastEntryBuffer = Unpooled.wrappedBuffer(readLacResponse.getLastEntryBody().asReadOnlyByteBuffer());
            }

            int rc = logAndConvertStatus(status,
                                         BKException.Code.ReadException,
                                         "ledger", ledgerId);
            cb.readLacComplete(rc, ledgerId, lacBuffer.slice(),
                               lastEntryBuffer.slice(), ctx);
        }
    }

    // visible for testing
    class ReadCompletion extends CompletionValue {
        final ReadEntryCallback cb;

        public ReadCompletion(final CompletionKey key,
                              final ReadEntryCallback originalCallback,
                              final Object originalCtx,
                              long ledgerId, final long entryId) {
            super("Read", originalCtx, ledgerId, entryId,
                  readEntryOpLogger, readTimeoutOpLogger,
                  scheduleTimeout(key, readEntryTimeout));

            this.cb = new ReadEntryCallback() {
                    @Override
                    public void readEntryComplete(int rc, long ledgerId,
                                                  long entryId, ByteBuf buffer,
                                                  Object ctx) {
                        cancelTimeoutAndLogOp(rc);
                        originalCallback.readEntryComplete(rc,
                                                           ledgerId, entryId,
                                                           buffer, originalCtx);
                        key.release();
                    }
                };
        }

        @Override
        public void errorOut() {
            errorOut(BKException.Code.BookieHandleNotAvailableException);
        }

        @Override
        public void errorOut(final int rc) {
            errorOutAndRunCallback(
                    () -> cb.readEntryComplete(rc, ledgerId,
                                               entryId, null, ctx));
        }

        @Override
        public void handleV2Response(long ledgerId, long entryId,
                                     StatusCode status,
                                     BookieProtocol.Response response) {
            if (!(response instanceof BookieProtocol.ReadResponse)) {
                return;
            }
            BookieProtocol.ReadResponse readResponse = (BookieProtocol.ReadResponse) response;
            ByteBuf data = null;
            if (readResponse.hasData()) {
                data = readResponse.getData();
            }
            handleReadResponse(ledgerId, entryId, status, data,
                               INVALID_ENTRY_ID, -1L);
        }

        @Override
        public void handleV3Response(BookkeeperProtocol.Response response) {
            ReadResponse readResponse = response.getReadResponse();
            StatusCode status = response.getStatus() == StatusCode.EOK
                ? readResponse.getStatus() : response.getStatus();
            ByteBuf buffer = Unpooled.EMPTY_BUFFER;
            if (readResponse.hasBody()) {
                buffer = Unpooled.wrappedBuffer(readResponse.getBody().asReadOnlyByteBuffer());
            }
            long maxLAC = INVALID_ENTRY_ID;
            if (readResponse.hasMaxLAC()) {
                maxLAC = readResponse.getMaxLAC();
            }
            long lacUpdateTimestamp = -1L;
            if (readResponse.hasLacUpdateTimestamp()) {
                lacUpdateTimestamp = readResponse.getLacUpdateTimestamp();
            }
            handleReadResponse(readResponse.getLedgerId(),
                               readResponse.getEntryId(),
                               status, buffer, maxLAC, lacUpdateTimestamp);
        }

        private void handleReadResponse(long ledgerId,
                                        long entryId,
                                        StatusCode status,
                                        ByteBuf buffer,
                                        long maxLAC, // max known lac piggy-back from bookies
                                        long lacUpdateTimestamp) { // the timestamp when the lac is updated.
            int readableBytes = buffer == null ? 0 : buffer.readableBytes();
            int rc = logAndConvertStatus(status,
                                         BKException.Code.ReadException,
                                         "ledger", ledgerId,
                                         "entry", entryId,
                                         "entryLength", readableBytes);

            if(buffer != null) {
                buffer = buffer.slice();
            }
            if (maxLAC > INVALID_ENTRY_ID
                && (ctx instanceof ReadEntryCallbackCtx)) {
                ((ReadEntryCallbackCtx)ctx).setLastAddConfirmed(maxLAC);
            }
            if (lacUpdateTimestamp > -1L
                && (ctx instanceof ReadLastConfirmedAndEntryContext)) {
                ((ReadLastConfirmedAndEntryContext)ctx).setLacUpdateTimestamp(lacUpdateTimestamp);
            }
            cb.readEntryComplete(rc, ledgerId, entryId, buffer, ctx);
        }
    }

    class StartTLSCompletion extends CompletionValue {
        final StartTLSCallback cb;

        public StartTLSCompletion(final CompletionKey key) {
            super("StartTLS", null, -1, -1,
                  startTLSOpLogger, startTLSTimeoutOpLogger,
                  scheduleTimeout(key, startTLSTimeout));
            this.cb = new StartTLSCallback() {
                @Override
                public void startTLSComplete(int rc, Object ctx) {
                    cancelTimeoutAndLogOp(rc);
                    key.release();
                }
            };
        }

        @Override
        public void errorOut() {
            errorOut(BKException.Code.BookieHandleNotAvailableException);
        }

        @Override
        public void errorOut(final int rc) {
            failTLS(rc);
        }

        @Override
        public void handleV3Response(BookkeeperProtocol.Response response) {
            StatusCode status = response.getStatus();

            int rc = logAndConvertStatus(status,
                                         BKException.Code.SecurityException);

            // Cancel START_TLS request timeout
            cb.startTLSComplete(rc, null);

            if (state != ConnectionState.START_TLS) {
                LOG.error("Connection state changed before TLS response received");
                failTLS(BKException.Code.BookieHandleNotAvailableException);
            } else if (status != StatusCode.EOK) {
                LOG.error("Client received error {} during TLS negotiation", status);
                failTLS(BKException.Code.SecurityException);
            } else {
                initTLSHandshake();
            }
        }

    }

    // visible for testing
    class GetBookieInfoCompletion extends CompletionValue {
        final GetBookieInfoCallback cb;

        public GetBookieInfoCompletion(final CompletionKey key,
                                       final GetBookieInfoCallback origCallback,
                                       final Object origCtx) {
            super("GetBookieInfo", origCtx, 0L, 0L,
                  getBookieInfoOpLogger, getBookieInfoTimeoutOpLogger,
                  scheduleTimeout(key, getBookieInfoTimeout));
            this.cb = new GetBookieInfoCallback() {
                @Override
                public void getBookieInfoComplete(int rc, BookieInfo bInfo,
                                                  Object ctx) {
                    cancelTimeoutAndLogOp(rc);
                    origCallback.getBookieInfoComplete(rc, bInfo, origCtx);
                    key.release();
                }
            };
        }

        @Override
        public void errorOut() {
            errorOut(BKException.Code.BookieHandleNotAvailableException);
        }

        @Override
        public void errorOut(final int rc) {
            errorOutAndRunCallback(
                    () -> cb.getBookieInfoComplete(rc, new BookieInfo(), ctx));
        }

        @Override
        public void handleV3Response(BookkeeperProtocol.Response response) {
            GetBookieInfoResponse getBookieInfoResponse
                = response.getGetBookieInfoResponse();
            StatusCode status = response.getStatus() == StatusCode.EOK
                ? getBookieInfoResponse.getStatus() : response.getStatus();

            long freeDiskSpace = getBookieInfoResponse.getFreeDiskSpace();
            long totalDiskSpace = getBookieInfoResponse.getTotalDiskCapacity();

            int rc = logAndConvertStatus(status,
                                         BKException.Code.ReadException,
                                         "freeDisk", freeDiskSpace,
                                         "totalDisk", totalDiskSpace);
            cb.getBookieInfoComplete(rc,
                                     new BookieInfo(totalDiskSpace,
                                                    freeDiskSpace), ctx);
        }
    }

    private final Recycler<AddCompletion> ADD_COMPLETION_RECYCLER = new Recycler<AddCompletion>() {
            protected AddCompletion newObject(Recycler.Handle<AddCompletion> handle) {
                return new AddCompletion(handle);
            }
        };

    AddCompletion acquireAddCompletion(final CompletionKey key,
                                       final WriteCallback originalCallback,
                                       final Object originalCtx,
                                       final long ledgerId, final long entryId) {
        AddCompletion completion = ADD_COMPLETION_RECYCLER.get();
        completion.reset(key, originalCallback, originalCtx, ledgerId, entryId);
        return completion;
    }

    // visible for testing
    class AddCompletion extends CompletionValue implements WriteCallback {
        final Recycler.Handle<AddCompletion> handle;

        CompletionKey key = null;
        WriteCallback originalCallback = null;

        AddCompletion(Recycler.Handle<AddCompletion> handle) {
            super("Add", null, -1, -1,
                  addEntryOpLogger, addTimeoutOpLogger, null);
            this.handle = handle;
        }

        void reset(final CompletionKey key,
                   final WriteCallback originalCallback,
                   final Object originalCtx,
                   final long ledgerId, final long entryId) {
            this.key = key;
            this.originalCallback = originalCallback;
            this.ctx = originalCtx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.startTime = MathUtils.nowInNano();
            this.timeout = scheduleTimeout(key, addEntryTimeout);
        }

        @Override
        public void writeComplete(int rc, long ledgerId, long entryId,
                                  BookieSocketAddress addr,
                                  Object ctx) {
            cancelTimeoutAndLogOp(rc);
            originalCallback.writeComplete(rc, ledgerId, entryId, addr, ctx);
            key.release();
            handle.recycle(this);
        }

        @Override
        public void errorOut() {
            errorOut(BKException.Code.BookieHandleNotAvailableException);
        }

        @Override
        public void errorOut(final int rc) {
            errorOutAndRunCallback(
                    () -> writeComplete(rc, ledgerId, entryId, addr, ctx));
        }

        @Override
        public void handleV2Response(
                long ledgerId, long entryId, StatusCode status,
                BookieProtocol.Response response) {
            handleResponse(ledgerId, entryId, status);
        }

        @Override
        public void handleV3Response(
                BookkeeperProtocol.Response response) {
            AddResponse addResponse = response.getAddResponse();
            StatusCode status = response.getStatus() == StatusCode.EOK
                ? addResponse.getStatus() : response.getStatus();
            handleResponse(addResponse.getLedgerId(), addResponse.getEntryId(),
                           status);
        }

        private void handleResponse(long ledgerId, long entryId,
                                    StatusCode status) {
            int rc = logAndConvertStatus(status,
                                         BKException.Code.WriteException,
                                         "ledger", ledgerId,
                                         "entry", entryId);
            writeComplete(rc, ledgerId, entryId, addr, ctx);
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
        OperationType operationType;

        CompletionKey(long txnId,
                      OperationType operationType) {
            this.txnId = txnId;
            this.operationType = operationType;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }
            CompletionValue completion = completionObjects.remove(this);
            if (completion != null) {
                completion.timeout();
            }
        }

        public void release() {}
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
            case ETOOMANYREQUESTS:
                rcToRet = BKException.Code.TooManyRequestsException;
                break;
            default:
                break;
        }
        return rcToRet;
    }

    private long getTxnId() {
        return txnIdGenerator.incrementAndGet();
    }

    private final Recycler<V2CompletionKey> V2_KEY_RECYCLER = new Recycler<V2CompletionKey>() {
            protected V2CompletionKey newObject(
                    Recycler.Handle<V2CompletionKey> handle) {
                return new V2CompletionKey(handle);
            }
        };

    V2CompletionKey acquireV2Key(long ledgerId, long entryId,
                             OperationType operationType) {
        V2CompletionKey key = V2_KEY_RECYCLER.get();
        key.reset(ledgerId, entryId, operationType);
        return key;
    }

    private class V2CompletionKey extends CompletionKey {
        private final Handle<V2CompletionKey> recyclerHandle;
        long ledgerId;
        long entryId;

        private V2CompletionKey(Handle<V2CompletionKey> handle) {
            super(-1, null);
            this.recyclerHandle = handle;
        }

        void reset(long ledgerId, long entryId, OperationType operationType) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.operationType = operationType;
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof V2CompletionKey)) {
                return  false;
            }
            V2CompletionKey that = (V2CompletionKey) object;
            return this.entryId == that.entryId
                && this.ledgerId == that.ledgerId
                && this.operationType == that.operationType;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(ledgerId) * 31 + Long.hashCode(entryId);
        }

        @Override
        public String toString() {
            return String.format("%d:%d %s", ledgerId, entryId, operationType);
        }

        @Override
        public void release() {
            recyclerHandle.recycle(this);
        }
    }

    public class ConnectionFutureListener implements ChannelFutureListener {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            LOG.debug("Channel connected ({}) {}", future.isSuccess(), future.channel());
            int rc;
            Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;

            synchronized (PerChannelBookieClient.this) {
                if (future.isSuccess() && state == ConnectionState.CONNECTING && future.channel().isActive()) {
                    LOG.info("Successfully connected to bookie: {}", future.channel());
                    rc = BKException.Code.OK;
                    channel = future.channel();
                    if (shFactory != null) {
                        initiateTLS();
                        return;
                    } else {
                        LOG.info("Successfully connected to bookie: " + addr);
                        state = ConnectionState.CONNECTED;
                    }
                } else if (future.isSuccess() && state == ConnectionState.START_TLS) {
                    rc = BKException.Code.OK;
                    LOG.info("Successfully connected to bookie using TLS: " + addr);

                    state = ConnectionState.CONNECTED;
                    AuthHandler.ClientSideHandler authHandler = future.channel().pipeline()
                            .get(AuthHandler.ClientSideHandler.class);
                    authHandler.authProvider.onProtocolUpgrade();
                } else if (future.isSuccess() && (state == ConnectionState.CLOSED
                    || state == ConnectionState.DISCONNECTED)) {
                    LOG.warn("Closed before connection completed, clean up: {}, current state {}",
                            future.channel(), state);
                    closeChannel(future.channel());
                    rc = BKException.Code.BookieHandleNotAvailableException;
                    channel = null;
                } else if (future.isSuccess() && state == ConnectionState.CONNECTED) {
                    LOG.debug("Already connected with another channel({}), so close the new channel({})",
                            channel, future.channel());
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
                pendingOps = new ArrayDeque<>();
            }

            for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
                pendingOp.operationComplete(rc, PerChannelBookieClient.this);
            }
        }
    }

    private void initiateTLS() {
        LOG.info("Initializing TLS to {}",channel);
        assert state == ConnectionState.CONNECTING;
        final long txnId = getTxnId();
        final CompletionKey completionKey = new V3CompletionKey(txnId, OperationType.START_TLS);
        completionObjects.put(completionKey,
                              new StartTLSCompletion(completionKey));
        BookkeeperProtocol.Request.Builder h = BookkeeperProtocol.Request.newBuilder();
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.START_TLS)
                .setTxnId(txnId);
        h.setHeader(headerBuilder.build());
        h.setStartTLSRequest(BookkeeperProtocol.StartTLSRequest.newBuilder().build());
        state = ConnectionState.START_TLS;
        writeAndFlush(channel, completionKey, h.build());
    }

    private void failTLS(int rc) {
        LOG.error("TLS failure on: {}, rc: {}", channel, rc);
        Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;
        synchronized(this) {
            disconnect();
            oldPendingOps = pendingOps;
            pendingOps = new ArrayDeque<>();
        }
        for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
            pendingOp.operationComplete(rc, null);
        }
    }
}
