/*
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

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.Errors.NativeIoException;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.incubator.channel.uring.IOUringChannelOption;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringSocketChannel;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.security.cert.Certificate;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiPredicate;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import lombok.SneakyThrows;
import org.apache.bookkeeper.auth.BookKeeperPrincipal;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperClientStats;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.MdcUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.BatchedReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ForceLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetListOfEntriesOfLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ForceLedgerRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetListOfEntriesOfLedgerRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadLacRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.proto.BookkeeperProtocol.WriteLacRequest;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.tls.SecurityHandlerFactory.NodeType;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.util.collections.ConcurrentOpenHashMap;
import org.apache.bookkeeper.util.collections.SynchronizedHashMultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This class manages all details of connection to a particular bookie. It also
 * has reconnect logic if a connection to a bookie fails.
 */
@StatsDoc(
    name = BookKeeperClientStats.CHANNEL_SCOPE,
    help = "Per channel bookie client stats"
)
@Sharable
public class PerChannelBookieClient extends ChannelInboundHandlerAdapter {

    static final Logger LOG = LoggerFactory.getLogger(PerChannelBookieClient.class);

    // this set contains the bookie error return codes that we do not consider for a bookie to be "faulty"
    protected static final Set<Integer> EXPECTED_BK_OPERATION_ERRORS = Collections.unmodifiableSet(Sets
            .newHashSet(BKException.Code.BookieHandleNotAvailableException,
                        BKException.Code.NoSuchEntryException,
                        BKException.Code.NoSuchLedgerExistsException,
                        BKException.Code.LedgerFencedException,
                        BKException.Code.LedgerExistException,
                        BKException.Code.DuplicateEntryIdException,
                        BKException.Code.WriteOnReadOnlyBookieException));
    private static final int DEFAULT_HIGH_PRIORITY_VALUE = 100; // We may add finer grained priority later.
    private static final AtomicLong txnIdGenerator = new AtomicLong(0);
    static final String CONSOLIDATION_HANDLER_NAME = "consolidation";

    final BookieId bookieId;
    final BookieAddressResolver bookieAddressResolver;
    final EventLoopGroup eventLoopGroup;
    final ByteBufAllocator allocator;
    final OrderedExecutor executor;
    final long addEntryTimeoutNanos;
    final long readEntryTimeoutNanos;
    final int maxFrameSize;
    final long getBookieInfoTimeoutNanos;
    final int startTLSTimeout;

    private final ConcurrentOpenHashMap<CompletionKey, CompletionValue> completionObjects =
            ConcurrentOpenHashMap.<CompletionKey, CompletionValue>newBuilder().autoShrink(true).build();

    // Map that hold duplicated read requests. The idea is to only use this map (synchronized) when there is a duplicate
    // read request for the same ledgerId/entryId
    private final SynchronizedHashMultiMap<CompletionKey, CompletionValue> completionObjectsV2Conflicts =
        new SynchronizedHashMultiMap<>();

    private final StatsLogger statsLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_READ_OP,
        help = "channel stats of read entries requests"
    )
    protected final OpStatsLogger readEntryOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_TIMEOUT_READ,
        help = "timeout stats of read entries requests"
    )
    protected final OpStatsLogger readTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_ADD_OP,
        help = "channel stats of add entries requests"
    )
    protected final OpStatsLogger addEntryOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_WRITE_LAC_OP,
        help = "channel stats of write_lac requests"
    )
    protected final OpStatsLogger writeLacOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_FORCE_OP,
        help = "channel stats of force requests"
    )
    protected final OpStatsLogger forceLedgerOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_READ_LAC_OP,
        help = "channel stats of read_lac requests"
    )
    protected final OpStatsLogger readLacOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_TIMEOUT_ADD,
        help = "timeout stats of add entries requests"
    )
    protected final OpStatsLogger addTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_TIMEOUT_WRITE_LAC,
        help = "timeout stats of write_lac requests"
    )
    protected final OpStatsLogger writeLacTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_TIMEOUT_FORCE,
        help = "timeout stats of force requests"
    )
    protected final OpStatsLogger forceLedgerTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_TIMEOUT_READ_LAC,
        help = "timeout stats of read_lac requests"
    )
    protected final OpStatsLogger readLacTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.GET_BOOKIE_INFO_OP,
        help = "channel stats of get_bookie_info requests"
    )
    protected final OpStatsLogger getBookieInfoOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.TIMEOUT_GET_BOOKIE_INFO,
        help = "timeout stats of get_bookie_info requests"
    )
    protected final OpStatsLogger getBookieInfoTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_START_TLS_OP,
        help = "channel stats of start_tls requests"
    )
    protected final OpStatsLogger startTLSOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_TIMEOUT_START_TLS_OP,
        help = "timeout stats of start_tls requests"
    )
    protected final OpStatsLogger startTLSTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CLIENT_CONNECT_TIMER,
        help = "channel stats of connect requests"
    )
    private final OpStatsLogger connectTimer;
    protected final OpStatsLogger getListOfEntriesOfLedgerCompletionOpLogger;
    protected final OpStatsLogger getListOfEntriesOfLedgerCompletionTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.NETTY_EXCEPTION_CNT,
        help = "the number of exceptions received from this channel"
    )
    private final Counter exceptionCounter;
    @StatsDoc(
        name = BookKeeperClientStats.ADD_OP_OUTSTANDING,
        help = "the number of outstanding add_entry requests"
    )
    protected final Counter addEntryOutstanding;
    @StatsDoc(
        name = BookKeeperClientStats.READ_OP_OUTSTANDING,
        help = "the number of outstanding add_entry requests"
    )
    protected final Counter readEntryOutstanding;
    /* collect stats on all Ops that flows through netty pipeline */
    @StatsDoc(
        name = BookKeeperClientStats.NETTY_OPS,
        help = "channel stats for all operations flowing through netty pipeline"
    )
    private final OpStatsLogger nettyOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.ACTIVE_NON_TLS_CHANNEL_COUNTER,
        help = "the number of active non-tls channels"
    )
    private final Counter activeNonTlsChannelCounter;
    @StatsDoc(
        name = BookKeeperClientStats.ACTIVE_TLS_CHANNEL_COUNTER,
        help = "the number of active tls channels"
    )
    private final Counter activeTlsChannelCounter;
    @StatsDoc(
        name = BookKeeperClientStats.FAILED_CONNECTION_COUNTER,
        help = "the number of failed connections"
    )
    private final Counter failedConnectionCounter;
    @StatsDoc(
        name = BookKeeperClientStats.FAILED_TLS_HANDSHAKE_COUNTER,
        help = "the number of failed tls handshakes"
    )
    private final Counter failedTlsHandshakeCounter;

    private final boolean useV2WireProtocol;
    protected final boolean preserveMdcForTaskExecution;

    /**
     * The following member variables do not need to be concurrent, or volatile
     * because they are always updated under a lock.
     */
    private volatile Queue<GenericCallback<PerChannelBookieClient>> pendingOps =
            new ArrayDeque<GenericCallback<PerChannelBookieClient>>();
    volatile Channel channel = null;
    private final ClientConnectionPeer connectionPeer;
    private volatile BookKeeperPrincipal authorizedId = BookKeeperPrincipal.ANONYMOUS;

    @SneakyThrows
    private FailedChannelFutureImpl processBookieNotResolvedError(long startTime,
            BookieAddressResolver.BookieIdNotResolvedException err) {
        FailedChannelFutureImpl failedFuture = new FailedChannelFutureImpl(err);
        contextPreservingListener(new ConnectionFutureListener(startTime)).operationComplete(failedFuture);
        return failedFuture;
    }

    enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED, CLOSED, START_TLS
    }

    volatile ConnectionState state;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
    protected final ClientConfiguration conf;

    private final PerChannelBookieClientPool pcbcPool;
    private final ClientAuthProvider.Factory authProviderFactory;
    private final ExtensionRegistry extRegistry;
    private final SecurityHandlerFactory shFactory;
    private volatile boolean isWritable = true;
    private long lastBookieUnavailableLogTimestamp = 0;

    public PerChannelBookieClient(OrderedExecutor executor, EventLoopGroup eventLoopGroup,
                                  BookieId addr, BookieAddressResolver bookieAddressResolver) throws SecurityException {
        this(new ClientConfiguration(), executor, eventLoopGroup, addr, NullStatsLogger.INSTANCE, null, null,
                null, bookieAddressResolver);
    }

    public PerChannelBookieClient(OrderedExecutor executor, EventLoopGroup eventLoopGroup,
                                  BookieId bookieId,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry, BookieAddressResolver bookieAddressResolver)
            throws SecurityException {
        this(new ClientConfiguration(), executor, eventLoopGroup, bookieId,
                NullStatsLogger.INSTANCE,
                authProviderFactory, extRegistry, null, bookieAddressResolver);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedExecutor executor,
                                  EventLoopGroup eventLoopGroup, BookieId bookieId,
                                  StatsLogger parentStatsLogger, ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry,
                                  PerChannelBookieClientPool pcbcPool, BookieAddressResolver bookieAddressResolver)
            throws SecurityException {
        this(conf, executor, eventLoopGroup, UnpooledByteBufAllocator.DEFAULT, bookieId, NullStatsLogger.INSTANCE,
                authProviderFactory, extRegistry, pcbcPool, null, bookieAddressResolver);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedExecutor executor,
                                  EventLoopGroup eventLoopGroup,
                                  ByteBufAllocator allocator,
                                  BookieId bookieId,
                                  StatsLogger parentStatsLogger, ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry,
                                  PerChannelBookieClientPool pcbcPool,
                                  SecurityHandlerFactory shFactory,
                                  BookieAddressResolver bookieAddressResolver) throws SecurityException {
        this.maxFrameSize = conf.getNettyMaxFrameSizeBytes();
        this.conf = conf;
        this.bookieId = bookieId;
        this.bookieAddressResolver = bookieAddressResolver;
        this.executor = executor;
        if (LocalBookiesRegistry.isLocalBookie(bookieId)) {
            this.eventLoopGroup = new DefaultEventLoopGroup();
        } else {
            this.eventLoopGroup = eventLoopGroup;
        }
        this.allocator = allocator;
        this.state = ConnectionState.DISCONNECTED;
        this.addEntryTimeoutNanos = TimeUnit.SECONDS.toNanos(conf.getAddEntryTimeout());
        this.readEntryTimeoutNanos = TimeUnit.SECONDS.toNanos(conf.getReadEntryTimeout());
        this.getBookieInfoTimeoutNanos = TimeUnit.SECONDS.toNanos(conf.getBookieInfoTimeout());
        this.startTLSTimeout = conf.getStartTLSTimeout();
        this.useV2WireProtocol = conf.getUseV2WireProtocol();
        this.preserveMdcForTaskExecution = conf.getPreserveMdcForTaskExecution();

        this.authProviderFactory = authProviderFactory;
        this.extRegistry = extRegistry;
        this.shFactory = shFactory;
        if (shFactory != null) {
            shFactory.init(NodeType.Client, conf, allocator);
        }

        this.statsLogger = parentStatsLogger.scope(BookKeeperClientStats.CHANNEL_SCOPE)
            .scopeLabel(BookKeeperClientStats.BOOKIE_LABEL, bookieId.toString());

        readEntryOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_READ_OP);
        addEntryOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_ADD_OP);
        writeLacOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_WRITE_LAC_OP);
        forceLedgerOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_FORCE_OP);
        readLacOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_READ_LAC_OP);
        getBookieInfoOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.GET_BOOKIE_INFO_OP);
        getListOfEntriesOfLedgerCompletionOpLogger = statsLogger
                .getOpStatsLogger(BookKeeperClientStats.GET_LIST_OF_ENTRIES_OF_LEDGER_OP);
        readTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_READ);
        addTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_ADD);
        writeLacTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_WRITE_LAC);
        forceLedgerTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_FORCE);
        readLacTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_READ_LAC);
        getBookieInfoTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.TIMEOUT_GET_BOOKIE_INFO);
        startTLSOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_START_TLS_OP);
        startTLSTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_START_TLS_OP);
        getListOfEntriesOfLedgerCompletionTimeoutOpLogger = statsLogger
                .getOpStatsLogger(BookKeeperClientStats.TIMEOUT_GET_LIST_OF_ENTRIES_OF_LEDGER);
        exceptionCounter = statsLogger.getCounter(BookKeeperClientStats.NETTY_EXCEPTION_CNT);
        connectTimer = statsLogger.getOpStatsLogger(BookKeeperClientStats.CLIENT_CONNECT_TIMER);
        addEntryOutstanding = statsLogger.getCounter(BookKeeperClientStats.ADD_OP_OUTSTANDING);
        readEntryOutstanding = statsLogger.getCounter(BookKeeperClientStats.READ_OP_OUTSTANDING);
        nettyOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.NETTY_OPS);
        activeNonTlsChannelCounter = statsLogger.getCounter(BookKeeperClientStats.ACTIVE_NON_TLS_CHANNEL_COUNTER);
        activeTlsChannelCounter = statsLogger.getCounter(BookKeeperClientStats.ACTIVE_TLS_CHANNEL_COUNTER);
        failedConnectionCounter = statsLogger.getCounter(BookKeeperClientStats.FAILED_CONNECTION_COUNTER);
        failedTlsHandshakeCounter = statsLogger.getCounter(BookKeeperClientStats.FAILED_TLS_HANDSHAKE_COUNTER);

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
                    c.close().addListener(x -> makeWritable());
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

    protected long getNumPendingCompletionRequests() {
        return completionObjects.size();
    }

    protected ChannelFuture connect() {
        final long startTime = MathUtils.nowInNano();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connecting to bookie: {}", bookieId);
        }
        BookieSocketAddress addr;
        try {
            addr = bookieAddressResolver.resolve(bookieId);
        } catch (BookieAddressResolver.BookieIdNotResolvedException err) {
            LOG.error("Cannot connect to {} as endpoint resolution failed (probably bookie is down) err {}",
                    bookieId, err.toString());
            return processBookieNotResolvedError(startTime, err);
        }

        // Set up the ClientBootStrap so we can create a new Channel connection to the bookie.
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        if (eventLoopGroup instanceof IOUringEventLoopGroup) {
            bootstrap.channel(IOUringSocketChannel.class);
            try {
                bootstrap.option(IOUringChannelOption.TCP_USER_TIMEOUT, conf.getTcpUserTimeoutMillis());
            } catch (NoSuchElementException e) {
                // Property not set, so keeping default value.
            }
        } else if (eventLoopGroup instanceof EpollEventLoopGroup) {
            bootstrap.channel(EpollSocketChannel.class);
            try {
                // For Epoll channels, configure the TCP user timeout.
                bootstrap.option(EpollChannelOption.TCP_USER_TIMEOUT, conf.getTcpUserTimeoutMillis());
            } catch (NoSuchElementException e) {
                // Property not set, so keeping default value.
            }
        } else if (eventLoopGroup instanceof DefaultEventLoopGroup) {
            bootstrap.channel(LocalChannel.class);
        } else {
            bootstrap.channel(NioSocketChannel.class);
        }

        bootstrap.option(ChannelOption.ALLOCATOR, this.allocator);
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
                pipeline.addLast(CONSOLIDATION_HANDLER_NAME, new FlushConsolidationHandler(1024, true));
                pipeline.addLast("bytebufList", ByteBufList.ENCODER);
                pipeline.addLast("lengthbasedframedecoder",
                        new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
                pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.RequestEncoder(extRegistry));
                pipeline.addLast(
                    "bookieProtoDecoder",
                    new BookieProtoEncoding.ResponseDecoder(extRegistry, useV2WireProtocol, shFactory != null));
                pipeline.addLast("authHandler", new AuthHandler.ClientSideHandler(authProviderFactory, txnIdGenerator,
                            connectionPeer, useV2WireProtocol));
                pipeline.addLast("mainhandler", PerChannelBookieClient.this);
            }
        });

        SocketAddress bookieAddr = addr.getSocketAddress();
        if (eventLoopGroup instanceof DefaultEventLoopGroup) {
            bookieAddr = new LocalAddress(bookieId.toString());
        }

        ChannelFuture future = bootstrap.connect(bookieAddr);
        addChannelListeners(future, startTime);
        return future;
    }

    protected void addChannelListeners(ChannelFuture future, long connectStartTime) {
        future.addListener(contextPreservingListener(new ConnectionFutureListener(connectStartTime)));
        future.addListener(x -> makeWritable());
    }

    void cleanDisconnectAndClose() {
        disconnect();
        close();
    }

    /**
     *
     * @return boolean, true is PCBC is writable
     */
    public boolean isWritable() {
        return isWritable;
    }

    public void setWritable(boolean val) {
        isWritable = val;
    }

    private void makeWritable() {
        setWritable(true);
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

    void writeLac(final long ledgerId, final byte[] masterKey, final long lac, ByteBufList toSend, WriteLacCallback cb,
            Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new TxnCompletionKey(txnId,
                                                                OperationType.WRITE_LAC);
        // writeLac is mostly like addEntry hence uses addEntryTimeout
        completionObjects.put(completionKey,
                              new WriteLacCompletion(completionKey, cb,
                                                     ctx, ledgerId, this));

        // Build the request
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.WRITE_LAC)
                .setTxnId(txnId);
        ByteString body = ByteStringUtil.byteBufListToByteString(toSend);
        toSend.retain();
        Runnable cleanupActionFailedBeforeWrite = toSend::release;
        Runnable cleanupActionAfterWrite = cleanupActionFailedBeforeWrite;
        WriteLacRequest.Builder writeLacBuilder = WriteLacRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setLac(lac)
                .setMasterKey(UnsafeByteOperations.unsafeWrap(masterKey))
                .setBody(body);

        final Request writeLacRequest = withRequestContext(Request.newBuilder())
                .setHeader(headerBuilder)
                .setWriteLacRequest(writeLacBuilder)
                .build();
        writeAndFlush(channel, completionKey, writeLacRequest, false, cleanupActionFailedBeforeWrite,
                cleanupActionAfterWrite);
    }

    void forceLedger(final long ledgerId, ForceLedgerCallback cb, Object ctx) {
        if (useV2WireProtocol) {
                LOG.error("force is not allowed with v2 protocol");
                executor.executeOrdered(ledgerId, () -> {
                    cb.forceLedgerComplete(BKException.Code.IllegalOpException, ledgerId, bookieId, ctx);
                });
                return;
        }
        final long txnId = getTxnId();
        final CompletionKey completionKey = new TxnCompletionKey(txnId,
                                                                OperationType.FORCE_LEDGER);
        // force is mostly like addEntry hence uses addEntryTimeout
        completionObjects.put(completionKey,
                              new ForceLedgerCompletion(completionKey, cb,
                                                     ctx, ledgerId, this));

        // Build the request
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.FORCE_LEDGER)
                .setTxnId(txnId);
        ForceLedgerRequest.Builder writeLacBuilder = ForceLedgerRequest.newBuilder()
                .setLedgerId(ledgerId);

        final Request forceLedgerRequest = withRequestContext(Request.newBuilder())
                .setHeader(headerBuilder)
                .setForceLedgerRequest(writeLacBuilder)
                .build();
        writeAndFlush(channel, completionKey, forceLedgerRequest);
    }

    /**
     * This method should be called only after connection has been checked for
     * {@link #connectIfNeededAndDoOp(GenericCallback)}.
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
     * @param allowFastFail
     *          allowFastFail flag
     * @param writeFlags
     *          WriteFlags
     */
    void addEntry(final long ledgerId, byte[] masterKey, final long entryId, ReferenceCounted toSend, WriteCallback cb,
                  Object ctx, final int options, boolean allowFastFail, final EnumSet<WriteFlag> writeFlags) {
        Object request = null;
        CompletionKey completionKey = null;
        Runnable cleanupActionFailedBeforeWrite = null;
        Runnable cleanupActionAfterWrite = null;
        if (useV2WireProtocol) {
            if (writeFlags.contains(WriteFlag.DEFERRED_SYNC)) {
                LOG.error("invalid writeflags {} for v2 protocol", writeFlags);
                cb.writeComplete(BKException.Code.IllegalOpException, ledgerId, entryId, bookieId, ctx);
                return;
            }
            completionKey = EntryCompletionKey.acquireV2Key(ledgerId, entryId, OperationType.ADD_ENTRY);

            if (toSend instanceof ByteBuf) {
                ByteBuf byteBuf = ((ByteBuf) toSend).retainedDuplicate();
                request = byteBuf;
                cleanupActionFailedBeforeWrite = byteBuf::release;
            } else {
                ByteBufList byteBufList = (ByteBufList) toSend;
                byteBufList.retain();
                request = byteBufList;
                cleanupActionFailedBeforeWrite = byteBufList::release;
            }
        } else {
            final long txnId = getTxnId();
            completionKey = new TxnCompletionKey(txnId, OperationType.ADD_ENTRY);

            // Build the request and calculate the total size to be included in the packet.
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.ADD_ENTRY)
                    .setTxnId(txnId);
            if (((short) options & BookieProtocol.FLAG_HIGH_PRIORITY) == BookieProtocol.FLAG_HIGH_PRIORITY) {
                headerBuilder.setPriority(DEFAULT_HIGH_PRIORITY_VALUE);
            }

            ByteBufList bufToSend = (ByteBufList) toSend;
            ByteString body = ByteStringUtil.byteBufListToByteString(bufToSend);
            bufToSend.retain();
            cleanupActionFailedBeforeWrite = bufToSend::release;
            cleanupActionAfterWrite = cleanupActionFailedBeforeWrite;
            AddRequest.Builder addBuilder = AddRequest.newBuilder()
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId)
                    .setMasterKey(UnsafeByteOperations.unsafeWrap(masterKey))
                    .setBody(body);

            if (((short) options & BookieProtocol.FLAG_RECOVERY_ADD) == BookieProtocol.FLAG_RECOVERY_ADD) {
                addBuilder.setFlag(AddRequest.Flag.RECOVERY_ADD);
            }

            if (!writeFlags.isEmpty()) {
                // add flags only if needed, in order to be able to talk with old bookies
                addBuilder.setWriteFlags(WriteFlag.getWriteFlagsValue(writeFlags));
            }

            request = withRequestContext(Request.newBuilder())
                    .setHeader(headerBuilder)
                    .setAddRequest(addBuilder)
                    .build();
        }

        putCompletionKeyValue(completionKey,
                              AddCompletion.acquireAddCompletion(completionKey,
                                                   cb, ctx, ledgerId, entryId, this));
        // addEntry times out on backpressure
        writeAndFlush(channel, completionKey, request, allowFastFail, cleanupActionFailedBeforeWrite,
                cleanupActionAfterWrite);
    }

    public void readLac(final long ledgerId, ReadLacCallback cb, Object ctx) {
        Object request = null;
        CompletionKey completionKey = null;
        if (useV2WireProtocol) {
            request = BookieProtocol.ReadRequest.create(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                                                     ledgerId, 0, (short) 0, null);
            completionKey = EntryCompletionKey.acquireV2Key(ledgerId, 0, OperationType.READ_LAC);
        } else {
            final long txnId = getTxnId();
            completionKey = new TxnCompletionKey(txnId, OperationType.READ_LAC);

            // Build the request and calculate the total size to be included in the packet.
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.READ_LAC)
                    .setTxnId(txnId);
            ReadLacRequest.Builder readLacBuilder = ReadLacRequest.newBuilder()
                    .setLedgerId(ledgerId);
            request = withRequestContext(Request.newBuilder())
                    .setHeader(headerBuilder)
                    .setReadLacRequest(readLacBuilder)
                    .build();
        }
        putCompletionKeyValue(completionKey,
                              new ReadLacCompletion(completionKey, cb,
                                                    ctx, ledgerId, this));
        writeAndFlush(channel, completionKey, request);
    }

    public void getListOfEntriesOfLedger(final long ledgerId, GetListOfEntriesOfLedgerCallback cb) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new TxnCompletionKey(txnId, OperationType.GET_LIST_OF_ENTRIES_OF_LEDGER);
        completionObjects.put(completionKey, new GetListOfEntriesOfLedgerCompletion(
                completionKey, cb, ledgerId, this));

        // Build the request.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder().setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.GET_LIST_OF_ENTRIES_OF_LEDGER).setTxnId(txnId);

        GetListOfEntriesOfLedgerRequest.Builder getListOfEntriesOfLedgerRequestBuilder =
                GetListOfEntriesOfLedgerRequest.newBuilder().setLedgerId(ledgerId);

        final Request getListOfEntriesOfLedgerRequest = Request.newBuilder().setHeader(headerBuilder)
                .setGetListOfEntriesOfLedgerRequest(getListOfEntriesOfLedgerRequestBuilder).build();

        writeAndFlush(channel, completionKey, getListOfEntriesOfLedgerRequest);
    }

    /**
     * Long Poll Reads.
     */
    public void readEntryWaitForLACUpdate(final long ledgerId,
                                          final long entryId,
                                          final long previousLAC,
                                          final long timeOutInMillis,
                                          final boolean piggyBackEntry,
                                          ReadEntryCallback cb,
                                          Object ctx) {
        readEntryInternal(ledgerId, entryId, previousLAC, timeOutInMillis,
                          piggyBackEntry, cb, ctx, (short) 0, null, false);
    }

    /**
     * Normal Reads.
     */
    public void readEntry(final long ledgerId,
                          final long entryId,
                          ReadEntryCallback cb,
                          Object ctx,
                          int flags,
                          byte[] masterKey,
                          boolean allowFastFail) {
        readEntryInternal(ledgerId, entryId, null, null, false,
                          cb, ctx, (short) flags, masterKey, allowFastFail);
    }

    private void readEntryInternal(final long ledgerId,
                                   final long entryId,
                                   final Long previousLAC,
                                   final Long timeOutInMillis,
                                   final boolean piggyBackEntry,
                                   final ReadEntryCallback cb,
                                   final Object ctx,
                                   int flags,
                                   byte[] masterKey,
                                   boolean allowFastFail) {
        Object request = null;
        CompletionKey completionKey = null;
        if (useV2WireProtocol) {
            request = BookieProtocol.ReadRequest.create(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                    ledgerId, entryId, (short) flags, masterKey);
            completionKey = EntryCompletionKey.acquireV2Key(ledgerId, entryId, OperationType.READ_ENTRY);
        } else {
            final long txnId = getTxnId();
            completionKey = new TxnCompletionKey(txnId, OperationType.READ_ENTRY);

            // Build the request and calculate the total size to be included in the packet.
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.READ_ENTRY)
                    .setTxnId(txnId);
            if (((short) flags & BookieProtocol.FLAG_HIGH_PRIORITY) == BookieProtocol.FLAG_HIGH_PRIORITY) {
                headerBuilder.setPriority(DEFAULT_HIGH_PRIORITY_VALUE);
            }

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

            // Only one flag can be set on the read requests
            if (((short) flags & BookieProtocol.FLAG_DO_FENCING) == BookieProtocol.FLAG_DO_FENCING) {
                readBuilder.setFlag(ReadRequest.Flag.FENCE_LEDGER);
                if (masterKey == null) {
                    cb.readEntryComplete(BKException.Code.IncorrectParameterException,
                                         ledgerId, entryId, null, ctx);
                    return;
                }
                readBuilder.setMasterKey(ByteString.copyFrom(masterKey));
            }

            request = withRequestContext(Request.newBuilder())
                    .setHeader(headerBuilder)
                    .setReadRequest(readBuilder)
                    .build();
        }

        ReadCompletion readCompletion = new ReadCompletion(completionKey, cb, ctx, ledgerId, entryId, this);
        putCompletionKeyValue(completionKey, readCompletion);

        writeAndFlush(channel, completionKey, request, allowFastFail, null, null);
    }

    public void batchReadEntries(final long ledgerId,
                            final long startEntryId,
                            final int maxCount,
                            final long maxSize,
                            BatchedReadEntryCallback cb,
                            Object ctx,
                            int flags,
                            byte[] masterKey,
                            boolean allowFastFail) {

        batchReadEntriesInternal(ledgerId, startEntryId, maxCount, maxSize, null, null, false,
                cb, ctx, (short) flags, masterKey, allowFastFail);
    }

    private void batchReadEntriesInternal(final long ledgerId,
                                     final long startEntryId,
                                     final int maxCount,
                                     final long maxSize,
                                     final Long previousLAC,
                                     final Long timeOutInMillis,
                                     final boolean piggyBackEntry,
                                     final BatchedReadEntryCallback cb,
                                     final Object ctx,
                                     int flags,
                                     byte[] masterKey,
                                     boolean allowFastFail) {
        Object request;
        CompletionKey completionKey;
        final long txnId = getTxnId();
        if (useV2WireProtocol) {
            request = BookieProtocol.BatchedReadRequest.create(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                    ledgerId, startEntryId, (short) flags, masterKey, txnId, maxCount, maxSize);
            completionKey = new TxnCompletionKey(txnId, OperationType.BATCH_READ_ENTRY);
        } else {
            throw new UnsupportedOperationException("Unsupported batch read entry operation for v3 protocol.");
        }
        BatchedReadCompletion readCompletion = new BatchedReadCompletion(
                completionKey, cb, ctx, ledgerId, startEntryId, this);
        putCompletionKeyValue(completionKey, readCompletion);

        writeAndFlush(channel, completionKey, request, allowFastFail, null, null);
    }

    public void getBookieInfo(final long requested, GetBookieInfoCallback cb, Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new TxnCompletionKey(txnId, OperationType.GET_BOOKIE_INFO);
        completionObjects.put(completionKey,
                              new GetBookieInfoCompletion(
                                      completionKey, cb, ctx, this));

        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.GET_BOOKIE_INFO)
                .setTxnId(txnId);

        GetBookieInfoRequest.Builder getBookieInfoBuilder = GetBookieInfoRequest.newBuilder()
                .setRequested(requested);

        final Request getBookieInfoRequest = withRequestContext(Request.newBuilder())
                .setHeader(headerBuilder)
                .setGetBookieInfoRequest(getBookieInfoBuilder)
                .build();

        writeAndFlush(channel, completionKey, getBookieInfoRequest);
    }

    private static final BiPredicate<CompletionKey, CompletionValue> timeoutCheck = (key, value) -> {
        return value.maybeTimeout();
    };

    public void checkTimeoutOnPendingOperations() {
        int timedOutOperations = completionObjects.removeIf(timeoutCheck);

        timedOutOperations += completionObjectsV2Conflicts.removeIf(timeoutCheck);

        if (timedOutOperations > 0) {
            LOG.info("Timed-out {} operations to channel {} for {}",
                     timedOutOperations, channel, bookieId);
        }
    }

    /**
     * Disconnects the bookie client. It can be reused.
     */
    public void disconnect() {
        disconnect(true);
    }

    public void disconnect(boolean wait) {
        LOG.info("Disconnecting the per channel bookie client for {}", bookieId);
        closeInternal(false, wait);
    }

    /**
     * Closes the bookie client permanently. It cannot be reused.
     */
    public void close() {
        close(true);
    }

    public void close(boolean wait) {
        LOG.info("Closing the per channel bookie client for {}", bookieId);
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

        if (channel != null && channel.pipeline().get(SslHandler.class) != null) {
            activeTlsChannelCounter.dec();
        } else {
            activeNonTlsChannelCounter.dec();
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
            makeWritable();
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
        return c.close().addListener(x -> makeWritable());
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        final Channel c = channel;
        if (c == null || c.isWritable()) {
            makeWritable();
        }
        super.channelWritabilityChanged(ctx);
    }

    private void writeAndFlush(final Channel channel,
                               final CompletionKey key,
                               final Object request) {
        writeAndFlush(channel, key, request, false, null, null);
    }

    private void writeAndFlush(final Channel channel,
                           final CompletionKey key,
                           final Object request,
                               final boolean allowFastFail, final Runnable cleanupActionFailedBeforeWrite,
                               final Runnable cleanupActionAfterWrite) {
        if (channel == null) {
            LOG.warn("Operation {} failed: channel == null", StringUtils.requestToString(request));
            errorOut(key);
            if (cleanupActionFailedBeforeWrite != null) {
                cleanupActionFailedBeforeWrite.run();
            }
            return;
        }

        final boolean isChannelWritable = channel.isWritable();
        if (isWritable != isChannelWritable) {
            // isWritable is volatile so simple "isWritable = channel.isWritable()" would be slower
            isWritable = isChannelWritable;
        }

        if (allowFastFail && !isWritable) {
            LOG.warn("Operation {} failed: TooManyRequestsException",
                    StringUtils.requestToString(request));

            errorOut(key, BKException.Code.TooManyRequestsException);
            if (cleanupActionFailedBeforeWrite != null) {
                cleanupActionFailedBeforeWrite.run();
            }
            return;
        }

        try {
            final long startTime = MathUtils.nowInNano();

            ChannelPromise promise = channel.newPromise().addListener(future -> {
                try {
                    if (future.isSuccess()) {
                        nettyOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        CompletionValue completion = completionObjects.get(key);
                        if (completion != null) {
                            completion.setOutstanding();
                        }
                    } else {
                        nettyOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        errorOut(key);
                    }
                } finally {
                    if (cleanupActionAfterWrite != null) {
                        cleanupActionAfterWrite.run();
                    }
                }
            });
            channel.writeAndFlush(request, promise);
        } catch (Throwable e) {
            LOG.warn("Operation {} failed", StringUtils.requestToString(request), e);
            errorOut(key);
            if (cleanupActionFailedBeforeWrite != null) {
                cleanupActionFailedBeforeWrite.run();
            }
        }
    }

    void errorOut(final CompletionKey key) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing completion key: {}", key);
        }
        CompletionValue completion = completionObjects.remove(key);
        if (completion != null) {
            completion.errorOut();
        } else {
            // If there's no completion object here, try in the multimap
            completionObjectsV2Conflicts.removeAny(key).ifPresent(c -> c.errorOut());
        }
    }

    void errorOut(final CompletionKey key, final int rc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing completion key: {}", key);
        }
        CompletionValue completion = completionObjects.remove(key);
        if (completion != null) {
            completion.errorOut(rc);
        } else {
            // If there's no completion object here, try in the multimap
            completionObjectsV2Conflicts.removeAny(key).ifPresent(c -> c.errorOut(rc));
        }
    }

    /**
     * Errors out pending ops from per channel bookie client. As the channel
     * is being closed, all the operations waiting on the connection
     * will be sent to completion with error.
     */
    void errorOutPendingOps(int rc) {
        Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;
        synchronized (this) {
            oldPendingOps = pendingOps;
            pendingOps = new ArrayDeque<>();
        }

        for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
            pendingOp.operationComplete(rc, PerChannelBookieClient.this);
        }
    }

    /**
     * Errors out pending entries. We call this method from one thread to avoid
     * concurrent executions to QuorumOpMonitor (implements callbacks). It seems
     * simpler to call it from BookieHandle instead of calling directly from
     * here.
     */

    void errorOutOutstandingEntries(int rc) {
        Optional<CompletionKey> multikey = completionObjectsV2Conflicts.getAnyKey();
        while (multikey.isPresent()) {
            multikey.ifPresent(k -> errorOut(k, rc));
            multikey = completionObjectsV2Conflicts.getAnyKey();
        }
        for (CompletionKey key : completionObjects.keys()) {
            errorOut(key, rc);
        }
    }

    void recordError() {
        if (pcbcPool != null) {
            pcbcPool.recordError();
        }
    }

    /**
     * If our channel has disconnected, we just error out the pending entries.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("Disconnected from bookie channel {}", ctx.channel());
        if (ctx.channel() != null) {
            closeChannel(ctx.channel());
            if (ctx.channel().pipeline().get(SslHandler.class) != null) {
                activeTlsChannelCounter.dec();
            } else {
                activeNonTlsChannelCounter.dec();
            }
        }

        errorOutOutstandingEntries(BKException.Code.BookieHandleNotAvailableException);
        errorOutPendingOps(BKException.Code.BookieHandleNotAvailableException);

        synchronized (this) {
            if (this.channel == ctx.channel()
                && state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
                channel = null;
            }
        }

        // we don't want to reconnect right away. If someone sends a request to
        // this address, we will reconnect.
    }

    /**
     * Called by netty when an exception happens in one of the netty threads
     * (mostly due to what we do in the netty threads).
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        exceptionCounter.inc();
        if (cause instanceof CorruptedFrameException || cause instanceof TooLongFrameException) {
            LOG.error("Corrupted frame received from bookie: {}", ctx.channel());
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

        // TLSv1.3 doesn't throw SSLHandshakeException for certificate issues
        // see https://stackoverflow.com/a/62465859 for details about the reason
        // therefore catch SSLException to also cover TLSv1.3
        if (cause instanceof DecoderException && cause.getCause() instanceof SSLException) {
            LOG.error("TLS handshake failed", cause);
            errorOutPendingOps(BKException.Code.SecurityException);
            Channel c = ctx.channel();
            if (c != null) {
                closeChannel(c);
            }
            return;
        }

        if (cause instanceof IOException) {
            if (cause instanceof NativeIoException) {
                // Stack trace is not very interesting for native IO exceptio, the important part is in
                // the exception message
                LOG.warn("Exception caught on:{} cause: {}", ctx.channel(), cause.getMessage());
            } else {
                LOG.warn("Exception caught on:{} cause:", ctx.channel(), cause);
            }
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
     * Called by netty when a message is received on a channel.
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
        OperationType operationType = getOperationType(response.getOpCode());
        StatusCode status = getStatusCodeFromErrorCode(response.errorCode);

        CompletionKey key;
        if (OperationType.BATCH_READ_ENTRY == operationType) {
            key = new TxnCompletionKey(((BookieProtocol.BatchedReadResponse) response).getRequestId(), operationType);
        } else {
            key = EntryCompletionKey.acquireV2Key(response.ledgerId, response.entryId, operationType);
        }
        CompletionValue completionValue = getCompletionValue(key);
        key.release();

        if (null == completionValue) {
            // Unexpected response, so log it. The txnId should have been present.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected response received from bookie : " + bookieId + " for type : " + operationType
                        + " and ledger:entry : " + response.ledgerId + ":" + response.entryId);
            }
            response.release();
        } else {
            long orderingKey = completionValue.ledgerId;
            executor.executeOrdered(orderingKey,
                    ReadV2ResponseCallback.create(completionValue, response.ledgerId, response.entryId,
                                                  status, response));
        }
    }

    private static class ReadV2ResponseCallback implements Runnable {
        CompletionValue completionValue;
        long ledgerId;
        long entryId;
        StatusCode status;
        BookieProtocol.Response response;

        static ReadV2ResponseCallback create(CompletionValue completionValue, long ledgerId, long entryId,
                                             StatusCode status, BookieProtocol.Response response) {
            ReadV2ResponseCallback callback = RECYCLER.get();
            callback.completionValue = completionValue;
            callback.ledgerId = ledgerId;
            callback.entryId = entryId;
            callback.status = status;
            callback.response = response;
            return callback;
        }

        @Override
        public void run() {
            completionValue.handleV2Response(ledgerId, entryId, status, response);
            response.release();
            response.recycle();
            recycle();
        }

        void recycle() {
            completionValue = null;
            ledgerId = -1;
            entryId = -1;
            status = null;
            response = null;
            recyclerHandle.recycle(this);
        }

        private final Handle<ReadV2ResponseCallback> recyclerHandle;

        private ReadV2ResponseCallback(Handle<ReadV2ResponseCallback> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<ReadV2ResponseCallback> RECYCLER = new Recycler<ReadV2ResponseCallback>() {
            @Override
            protected ReadV2ResponseCallback newObject(Handle<ReadV2ResponseCallback> handle) {
                return new ReadV2ResponseCallback(handle);
            }
        };
    }

    private static OperationType getOperationType(byte opCode) {
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
            case BookieProtocol.BATCH_READ_ENTRY:
                return OperationType.BATCH_READ_ENTRY;
            default:
                throw new IllegalArgumentException("Invalid operation type " + opCode);
        }
    }

    private static StatusCode getStatusCodeFromErrorCode(int errorCode) {
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

    private void readV3Response(final Response response) {
        final BKPacketHeader header = response.getHeader();

        final CompletionKey key = newCompletionKey(header.getTxnId(), header.getOperation());
        final CompletionValue completionValue = completionObjects.get(key);

        if (null == completionValue) {
            // Unexpected response, so log it. The txnId should have been present.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected response received from bookie : " + bookieId + " for type : "
                        + header.getOperation() + " and txnId : " + header.getTxnId());
            }
        } else {
            long orderingKey = completionValue.ledgerId;
            executor.executeOrdered(orderingKey, new Runnable() {
                @Override
                public void run() {
                    completionValue.restoreMdcContext();
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

        completionObjects.remove(key);
    }

    void initTLSHandshake() {
        // create TLS handler
        PerChannelBookieClient parentObj = PerChannelBookieClient.this;
        SocketAddress socketAddress = channel.remoteAddress();
        InetSocketAddress address;
        if (socketAddress instanceof LocalAddress) {
            // if it is a local address, it looks like this: local:hostname:port
            String[] addr = socketAddress.toString().split(":");
            String hostname = addr[1];
            int port = Integer.parseInt(addr[2]);
            address = new InetSocketAddress(hostname, port);
        } else if (socketAddress instanceof InetSocketAddress) {
            address = (InetSocketAddress) socketAddress;
        } else {
            throw new RuntimeException("Unexpected socket address type");
        }
        LOG.info("Starting TLS handshake with {}:{}", address.getHostString(), address.getPort());
        SslHandler sslHandler = parentObj.shFactory.newTLSHandler(address.getHostName(), address.getPort());
        String sslHandlerName = parentObj.shFactory.getHandlerName();
        if (channel.pipeline().names().contains(CONSOLIDATION_HANDLER_NAME)) {
            channel.pipeline().addAfter(CONSOLIDATION_HANDLER_NAME, sslHandlerName, sslHandler);
        } else {
            // local transport doesn't contain FlushConsolidationHandler
            channel.pipeline().addFirst(sslHandlerName, sslHandler);
        }
        sslHandler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
                @Override
                public void operationComplete(Future<Channel> future) throws Exception {
                    int rc;
                    Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;

                    synchronized (PerChannelBookieClient.this) {
                        if (future.isSuccess() && state == ConnectionState.CONNECTING) {
                            LOG.error("Connection state changed before TLS handshake completed {}/{}", bookieId, state);
                            rc = BKException.Code.BookieHandleNotAvailableException;
                            closeChannel(channel);
                            channel = null;
                            if (state != ConnectionState.CLOSED) {
                                state = ConnectionState.DISCONNECTED;
                            }
                        } else if (future.isSuccess() && state == ConnectionState.START_TLS) {
                            rc = BKException.Code.OK;
                            LOG.info("Successfully connected to bookie using TLS: " + bookieId);

                            state = ConnectionState.CONNECTED;
                            AuthHandler.ClientSideHandler authHandler = future.get().pipeline()
                                    .get(AuthHandler.ClientSideHandler.class);
                            authHandler.authProvider.onProtocolUpgrade();
                            activeTlsChannelCounter.inc();
                        } else if (future.isSuccess()
                                && (state == ConnectionState.CLOSED || state == ConnectionState.DISCONNECTED)) {
                            LOG.warn("Closed before TLS handshake completed, clean up: {}, current state {}",
                                    channel, state);
                            closeChannel(channel);
                            rc = BKException.Code.BookieHandleNotAvailableException;
                            channel = null;
                        } else if (future.isSuccess() && state == ConnectionState.CONNECTED) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Already connected with another channel({}), "
                                                + "so close the new channel({})",
                                        channel, channel);
                            }
                            closeChannel(channel);
                            return; // pendingOps should have been completed when other channel connected
                        } else {
                            LOG.error("TLS handshake failed with bookie: {}/{}, current state {} : ",
                                    channel, bookieId, state, future.cause());
                            rc = BKException.Code.SecurityException;
                            closeChannel(channel);
                            channel = null;
                            if (state != ConnectionState.CLOSED) {
                                state = ConnectionState.DISCONNECTED;
                            }
                            failedTlsHandshakeCounter.inc();
                        }

                        // trick to not do operations under the lock, take the list
                        // of pending ops and assign it to a new variable, while
                        // emptying the pending ops by just assigning it to a new
                        // list
                        oldPendingOps = pendingOps;
                        pendingOps = new ArrayDeque<>();
                    }

                    makeWritable();

                    for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
                        pendingOp.operationComplete(rc, PerChannelBookieClient.this);
                    }
                }
            });
    }

    // visable for testing
    CompletionKey newCompletionKey(long txnId, OperationType operationType) {
        return new TxnCompletionKey(txnId, operationType);
    }

    private void putCompletionKeyValue(CompletionKey key, CompletionValue value) {
        CompletionValue existingValue = completionObjects.putIfAbsent(key, value);
        if (existingValue != null) { // will only happen for V2 keys, as V3 have unique txnid
            // There's a pending read request on same ledger/entry. Use the multimap to track all of them
            completionObjectsV2Conflicts.put(key, value);
        }
    }

    private CompletionValue getCompletionValue(CompletionKey key) {
        CompletionValue completionValue = completionObjects.remove(key);
        if (completionValue == null) {
            // If there's no completion object here, try in the multimap
            completionValue = completionObjectsV2Conflicts.removeAny(key).orElse(null);
        }
        return completionValue;
    }

    private long getTxnId() {
        return txnIdGenerator.incrementAndGet();
    }

    Request.Builder withRequestContext(Request.Builder builder) {
        if (preserveMdcForTaskExecution) {
            return appendRequestContext(builder);
        }
        return builder;
    }

    static Request.Builder appendRequestContext(Request.Builder builder) {
        final Map<String, String> mdcContextMap = MDC.getCopyOfContextMap();
        if (mdcContextMap == null || mdcContextMap.isEmpty()) {
            return builder;
        }
        for (Map.Entry<String, String> kv : mdcContextMap.entrySet()) {
            final BookkeeperProtocol.ContextPair context = BookkeeperProtocol.ContextPair.newBuilder()
                    .setKey(kv.getKey())
                    .setValue(kv.getValue())
                    .build();
            builder.addRequestContext(context);
        }
        return builder;
    }

    ChannelFutureListener contextPreservingListener(ChannelFutureListener listener) {
        return preserveMdcForTaskExecution ? new ContextPreservingFutureListener(listener) : listener;
    }

    /**
     * Decorator to preserve MDC for connection listener.
     */
    static class ContextPreservingFutureListener implements ChannelFutureListener {
        private final ChannelFutureListener listener;
        private final Map<String, String> mdcContextMap;

        ContextPreservingFutureListener(ChannelFutureListener listener) {
            this.listener = listener;
            this.mdcContextMap = MDC.getCopyOfContextMap();
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            MdcUtils.restoreContext(mdcContextMap);
            try {
                listener.operationComplete(future);
            } finally {
                MDC.clear();
            }
        }
    }

    /**
     * Connection listener.
     */
    class ConnectionFutureListener implements ChannelFutureListener {
        private final long startTime;

        ConnectionFutureListener(long startTime) {
            this.startTime = startTime;
        }

        @Override
        public void operationComplete(ChannelFuture future) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Channel connected ({}) {}", future.isSuccess(), future.channel());
            }
            int rc;
            Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;

            /* We fill in the timer based on whether the connect operation itself succeeded regardless of
             * whether there was a race */
            if (future.isSuccess()) {
                PerChannelBookieClient.this
                .connectTimer.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            } else {
                PerChannelBookieClient.this
                .connectTimer.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            }

            synchronized (PerChannelBookieClient.this) {
                if (future.isSuccess() && state == ConnectionState.CONNECTING && future.channel().isActive()) {
                    rc = BKException.Code.OK;
                    channel = future.channel();
                    if (shFactory != null) {
                        LOG.info("Successfully connected to bookie: {} {} initiate TLS", bookieId, future.channel());
                        makeWritable();
                        initiateTLS();
                        return;
                    } else {
                        LOG.info("Successfully connected to bookie: {} {}", bookieId, future.channel());
                        state = ConnectionState.CONNECTED;
                        activeNonTlsChannelCounter.inc();
                    }
                } else if (future.isSuccess() && state == ConnectionState.START_TLS) {
                    rc = BKException.Code.OK;
                    LOG.info("Successfully connected to bookie using TLS: " + bookieId);

                    state = ConnectionState.CONNECTED;
                    AuthHandler.ClientSideHandler authHandler = future.channel().pipeline()
                            .get(AuthHandler.ClientSideHandler.class);
                    authHandler.authProvider.onProtocolUpgrade();
                    activeTlsChannelCounter.inc();
                } else if (future.isSuccess() && (state == ConnectionState.CLOSED
                    || state == ConnectionState.DISCONNECTED)) {
                    LOG.warn("Closed before connection completed, clean up: {}, current state {}",
                            future.channel(), state);
                    closeChannel(future.channel());
                    rc = BKException.Code.BookieHandleNotAvailableException;
                    channel = null;
                } else if (future.isSuccess() && state == ConnectionState.CONNECTED) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Already connected with another channel({}), so close the new channel({})", channel,
                                future.channel());
                    }
                    closeChannel(future.channel());
                    return; // pendingOps should have been completed when other channel connected
                } else {
                    Throwable cause = future.cause();
                    if (cause instanceof UnknownHostException || cause instanceof NativeIoException) {
                        // Don't log stack trace for common errors
                        logBookieUnavailable(() -> LOG.warn("Could not connect to bookie: {}/{}, current state {} : {}",
                                future.channel(), bookieId, state, future.cause().getMessage()));
                    } else {
                        // Regular exceptions, include stack trace
                        logBookieUnavailable(() -> LOG.error("Could not connect to bookie: {}/{}, current state {} : ",
                                future.channel(), bookieId, state, future.cause()));
                    }

                    rc = BKException.Code.BookieHandleNotAvailableException;
                    Channel failedChannel = future.channel();
                    if (failedChannel != null) { // can be null in case of dummy failed ChannelFuture
                        closeChannel(failedChannel);
                    }
                    channel = null;
                    if (state != ConnectionState.CLOSED) {
                        state = ConnectionState.DISCONNECTED;
                    }
                    failedConnectionCounter.inc();
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

            makeWritable();
        }

        private void logBookieUnavailable(Runnable logger) {
            final long now = System.currentTimeMillis();
            if ((now - lastBookieUnavailableLogTimestamp) > conf.getClientConnectBookieUnavailableLogThrottlingMs()) {
                logger.run();
                lastBookieUnavailableLogTimestamp = now;
            }
        }
    }

    private void initiateTLS() {
        LOG.info("Initializing TLS to {}", channel);
        assert state == ConnectionState.CONNECTING;
        final long txnId = getTxnId();
        final CompletionKey completionKey = new TxnCompletionKey(txnId, OperationType.START_TLS);
        completionObjects.put(completionKey,
                              new StartTLSCompletion(completionKey, this));
        BookkeeperProtocol.Request.Builder h = withRequestContext(BookkeeperProtocol.Request.newBuilder());
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.START_TLS)
                .setTxnId(txnId);
        h.setHeader(headerBuilder.build());
        h.setStartTLSRequest(BookkeeperProtocol.StartTLSRequest.newBuilder().build());
        state = ConnectionState.START_TLS;
        writeAndFlush(channel, completionKey, h.build());
    }

    protected void failTLS(int rc) {
        LOG.error("TLS failure on: {}, rc: {}", channel, rc);
        Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;
        synchronized (this) {
            disconnect();
            oldPendingOps = pendingOps;
            pendingOps = new ArrayDeque<>();
        }
        for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
            pendingOp.operationComplete(rc, null);
        }
        failedTlsHandshakeCounter.inc();
    }

    private static class FailedChannelFutureImpl implements ChannelFuture {

        private final Throwable failureCause;
        public FailedChannelFutureImpl(Throwable failureCause) {
            this.failureCause = failureCause;
        }

        @Override
        public Channel channel() {
            // used only for log
            return null;
        }

        @Override
        public ChannelFuture addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        @SuppressWarnings({"unchecked", "varargs"})
        public ChannelFuture addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public ChannelFuture removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        @SuppressWarnings({"unchecked", "varargs"})
        public ChannelFuture removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public ChannelFuture sync() throws InterruptedException {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public ChannelFuture syncUninterruptibly() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public ChannelFuture await() throws InterruptedException {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public ChannelFuture awaitUninterruptibly() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public boolean isVoid() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public boolean isCancellable() {
            return false;
        }

        @Override
        public Throwable cause() {
            return failureCause;
        }

        @Override
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public boolean await(long timeoutMillis) throws InterruptedException {
            return true;
        }

        @Override
        public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public boolean awaitUninterruptibly(long timeoutMillis) {
            return true;
        }

        @Override
        public Void getNow() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            throw new ExecutionException(failureCause);
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            throw new ExecutionException(failureCause);
        }
    }
}
