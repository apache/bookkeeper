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

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.UnsafeByteOperations;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
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
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.Errors.NativeIoException;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
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
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiPredicate;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;

import org.apache.bookkeeper.auth.BookKeeperPrincipal;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperClientStats;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.MdcUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ForceLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetListOfEntriesOfLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallbackCtx;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.StartTLSCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ForceLedgerRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ForceLedgerResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetListOfEntriesOfLedgerRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetListOfEntriesOfLedgerResponse;
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
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.tls.SecurityHandlerFactory.NodeType;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
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
    private static final Set<Integer> expectedBkOperationErrors = Collections.unmodifiableSet(Sets
            .newHashSet(BKException.Code.BookieHandleNotAvailableException,
                        BKException.Code.NoSuchEntryException,
                        BKException.Code.NoSuchLedgerExistsException,
                        BKException.Code.LedgerFencedException,
                        BKException.Code.LedgerExistException,
                        BKException.Code.DuplicateEntryIdException,
                        BKException.Code.WriteOnReadOnlyBookieException));
    private static final int DEFAULT_HIGH_PRIORITY_VALUE = 100; // We may add finer grained priority later.
    private static final AtomicLong txnIdGenerator = new AtomicLong(0);

    final BookieSocketAddress addr;
    final EventLoopGroup eventLoopGroup;
    final ByteBufAllocator allocator;
    final OrderedExecutor executor;
    final long addEntryTimeoutNanos;
    final long readEntryTimeoutNanos;
    final int maxFrameSize;
    final int getBookieInfoTimeout;
    final int startTLSTimeout;

    private final ConcurrentOpenHashMap<CompletionKey, CompletionValue> completionObjects =
        new ConcurrentOpenHashMap<CompletionKey, CompletionValue>();

    // Map that hold duplicated read requests. The idea is to only use this map (synchronized) when there is a duplicate
    // read request for the same ledgerId/entryId
    private final SynchronizedHashMultiMap<CompletionKey, CompletionValue> completionObjectsV2Conflicts =
        new SynchronizedHashMultiMap<>();

    private final StatsLogger statsLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_READ_OP,
        help = "channel stats of read entries requests"
    )
    private final OpStatsLogger readEntryOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_TIMEOUT_READ,
        help = "timeout stats of read entries requests"
    )
    private final OpStatsLogger readTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_ADD_OP,
        help = "channel stats of add entries requests"
    )
    private final OpStatsLogger addEntryOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_WRITE_LAC_OP,
        help = "channel stats of write_lac requests"
    )
    private final OpStatsLogger writeLacOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_FORCE_OP,
        help = "channel stats of force requests"
    )
    private final OpStatsLogger forceLedgerOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_READ_LAC_OP,
        help = "channel stats of read_lac requests"
    )
    private final OpStatsLogger readLacOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_TIMEOUT_ADD,
        help = "timeout stats of add entries requests"
    )
    private final OpStatsLogger addTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_TIMEOUT_WRITE_LAC,
        help = "timeout stats of write_lac requests"
    )
    private final OpStatsLogger writeLacTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_TIMEOUT_FORCE,
        help = "timeout stats of force requests"
    )
    private final OpStatsLogger forceLedgerTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_TIMEOUT_READ_LAC,
        help = "timeout stats of read_lac requests"
    )
    private final OpStatsLogger readLacTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.GET_BOOKIE_INFO_OP,
        help = "channel stats of get_bookie_info requests"
    )
    private final OpStatsLogger getBookieInfoOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.TIMEOUT_GET_BOOKIE_INFO,
        help = "timeout stats of get_bookie_info requests"
    )
    private final OpStatsLogger getBookieInfoTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_START_TLS_OP,
        help = "channel stats of start_tls requests"
    )
    private final OpStatsLogger startTLSOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CHANNEL_TIMEOUT_START_TLS_OP,
        help = "timeout stats of start_tls requests"
    )
    private final OpStatsLogger startTLSTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.CLIENT_CONNECT_TIMER,
        help = "channel stats of connect requests"
    )
    private final OpStatsLogger connectTimer;
    private final OpStatsLogger getListOfEntriesOfLedgerCompletionOpLogger;
    private final OpStatsLogger getListOfEntriesOfLedgerCompletionTimeoutOpLogger;
    @StatsDoc(
        name = BookKeeperClientStats.NETTY_EXCEPTION_CNT,
        help = "the number of exceptions received from this channel"
    )
    private final Counter exceptionCounter;
    @StatsDoc(
        name = BookKeeperClientStats.ADD_OP_OUTSTANDING,
        help = "the number of outstanding add_entry requests"
    )
    private final Counter addEntryOutstanding;
    @StatsDoc(
        name = BookKeeperClientStats.READ_OP_OUTSTANDING,
        help = "the number of outstanding add_entry requests"
    )
    private final Counter readEntryOutstanding;
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
    private final boolean preserveMdcForTaskExecution;

    /**
     * The following member variables do not need to be concurrent, or volatile
     * because they are always updated under a lock.
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
    private volatile boolean isWritable = true;

    public PerChannelBookieClient(OrderedExecutor executor, EventLoopGroup eventLoopGroup,
                                  BookieSocketAddress addr) throws SecurityException {
        this(new ClientConfiguration(), executor, eventLoopGroup, addr, NullStatsLogger.INSTANCE, null, null,
                null);
    }

    public PerChannelBookieClient(OrderedExecutor executor, EventLoopGroup eventLoopGroup,
                                  BookieSocketAddress addr,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry) throws SecurityException {
        this(new ClientConfiguration(), executor, eventLoopGroup, addr, NullStatsLogger.INSTANCE,
                authProviderFactory, extRegistry, null);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedExecutor executor,
                                  EventLoopGroup eventLoopGroup, BookieSocketAddress addr,
                                  StatsLogger parentStatsLogger, ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry,
                                  PerChannelBookieClientPool pcbcPool) throws SecurityException {
        this(conf, executor, eventLoopGroup, UnpooledByteBufAllocator.DEFAULT, addr, NullStatsLogger.INSTANCE,
                authProviderFactory, extRegistry, pcbcPool, null);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedExecutor executor,
                                  EventLoopGroup eventLoopGroup,
                                  ByteBufAllocator allocator,
                                  BookieSocketAddress addr,
                                  StatsLogger parentStatsLogger, ClientAuthProvider.Factory authProviderFactory,
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
        this.allocator = allocator;
        this.state = ConnectionState.DISCONNECTED;
        this.addEntryTimeoutNanos = TimeUnit.SECONDS.toNanos(conf.getAddEntryTimeout());
        this.readEntryTimeoutNanos = TimeUnit.SECONDS.toNanos(conf.getReadEntryTimeout());
        this.getBookieInfoTimeout = conf.getBookieInfoTimeout();
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
            .scope(buildStatsLoggerScopeName(addr));

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

    public static String buildStatsLoggerScopeName(BookieSocketAddress addr) {
        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(addr.getHostName().replace('.', '_').replace('-', '_')).append("_").append(addr.getPort());
        return nameBuilder.toString();
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

    protected long getNumPendingCompletionRequests() {
        return completionObjects.size();
    }

    protected ChannelFuture connect() {
        final long startTime = MathUtils.nowInNano();
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

                pipeline.addLast("bytebufList", ByteBufList.ENCODER_WITH_SIZE);
                pipeline.addLast("lengthbasedframedecoder",
                        new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
                pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
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
            bookieAddr = addr.getLocalAddress();
        }

        ChannelFuture future = bootstrap.connect(bookieAddr);
        future.addListener(contextPreservingListener(new ConnectionFutureListener(startTime)));
        future.addListener(x -> makeWritable());
        return future;
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
        ByteString body;
        if (toSend.hasArray()) {
            body = UnsafeByteOperations.unsafeWrap(toSend.array(), toSend.arrayOffset(), toSend.readableBytes());
        } else if (toSend.size() == 1) {
            body = UnsafeByteOperations.unsafeWrap(toSend.getBuffer(0).nioBuffer());
        } else {
            body = UnsafeByteOperations.unsafeWrap(toSend.toArray());
        }
        WriteLacRequest.Builder writeLacBuilder = WriteLacRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setLac(lac)
                .setMasterKey(UnsafeByteOperations.unsafeWrap(masterKey))
                .setBody(body);

        final Request writeLacRequest = withRequestContext(Request.newBuilder())
                .setHeader(headerBuilder)
                .setWriteLacRequest(writeLacBuilder)
                .build();
        writeAndFlush(channel, completionKey, writeLacRequest);
    }

    void forceLedger(final long ledgerId, ForceLedgerCallback cb, Object ctx) {
        if (useV2WireProtocol) {
                LOG.error("force is not allowed with v2 protocol");
                executor.executeOrdered(ledgerId, () -> {
                    cb.forceLedgerComplete(BKException.Code.IllegalOpException, ledgerId, addr, ctx);
                });
                return;
        }
        final long txnId = getTxnId();
        final CompletionKey completionKey = new V3CompletionKey(txnId,
                                                                OperationType.FORCE_LEDGER);
        // force is mostly like addEntry hence uses addEntryTimeout
        completionObjects.put(completionKey,
                              new ForceLedgerCompletion(completionKey, cb,
                                                     ctx, ledgerId));

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
    void addEntry(final long ledgerId, byte[] masterKey, final long entryId, ByteBufList toSend, WriteCallback cb,
                  Object ctx, final int options, boolean allowFastFail, final EnumSet<WriteFlag> writeFlags) {
        Object request = null;
        CompletionKey completionKey = null;
        if (useV2WireProtocol) {
            if (writeFlags.contains(WriteFlag.DEFERRED_SYNC)) {
                LOG.error("invalid writeflags {} for v2 protocol", writeFlags);
                executor.executeOrdered(ledgerId, () -> {
                    cb.writeComplete(BKException.Code.IllegalOpException, ledgerId, entryId, addr, ctx);
                });
                return;
            }
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
            if (((short) options & BookieProtocol.FLAG_HIGH_PRIORITY) == BookieProtocol.FLAG_HIGH_PRIORITY) {
                headerBuilder.setPriority(DEFAULT_HIGH_PRIORITY_VALUE);
            }

            ByteString body = null;
            if (toSend.hasArray()) {
                body = UnsafeByteOperations.unsafeWrap(toSend.array(), toSend.arrayOffset(), toSend.readableBytes());
            } else {
                for (int i = 0; i < toSend.size(); i++) {
                    ByteString piece = UnsafeByteOperations.unsafeWrap(toSend.getBuffer(i).nioBuffer());
                    // use ByteString.concat to avoid byte[] allocation when toSend has multiple ByteBufs
                    body = (body == null) ? piece : body.concat(piece);
                }
            }
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
            // addEntry times out on backpressure
            writeAndFlush(c, completionKey, request, allowFastFail);
        }
    }

    public void readLac(final long ledgerId, ReadLacCallback cb, Object ctx) {
        Object request = null;
        CompletionKey completionKey = null;
        if (useV2WireProtocol) {
            request = new BookieProtocol.ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                                                     ledgerId, 0, (short) 0, null);
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
            request = withRequestContext(Request.newBuilder())
                    .setHeader(headerBuilder)
                    .setReadLacRequest(readLacBuilder)
                    .build();
        }
        putCompletionKeyValue(completionKey,
                              new ReadLacCompletion(completionKey, cb,
                                                    ctx, ledgerId));
        writeAndFlush(channel, completionKey, request);
    }

    public void getListOfEntriesOfLedger(final long ledgerId, GetListOfEntriesOfLedgerCallback cb) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new V3CompletionKey(txnId, OperationType.GET_LIST_OF_ENTRIES_OF_LEDGER);
        completionObjects.put(completionKey, new GetListOfEntriesOfLedgerCompletion(completionKey, cb, ledgerId));

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
            request = new BookieProtocol.ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                    ledgerId, entryId, (short) flags, masterKey);
            completionKey = acquireV2Key(ledgerId, entryId, OperationType.READ_ENTRY);
        } else {
            final long txnId = getTxnId();
            completionKey = new V3CompletionKey(txnId, OperationType.READ_ENTRY);

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

        ReadCompletion readCompletion = new ReadCompletion(completionKey, cb, ctx, ledgerId, entryId);
        putCompletionKeyValue(completionKey, readCompletion);

        writeAndFlush(channel, completionKey, request, allowFastFail);
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
                     timedOutOperations, channel, addr);
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
        writeAndFlush(channel, key, request, false);
    }

    private void writeAndFlush(final Channel channel,
                           final CompletionKey key,
                           final Object request,
                           final boolean allowFastFail) {
        if (channel == null) {
            LOG.warn("Operation {} failed: channel == null", StringUtils.requestToString(request));
            errorOut(key);
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
            return;
        }

        try {
            final long startTime = MathUtils.nowInNano();

            ChannelPromise promise = channel.newPromise().addListener(future -> {
                if (future.isSuccess()) {
                    nettyOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                    CompletionValue completion = completionObjects.get(key);
                    if (completion != null) {
                        completion.setOutstanding();
                    }
                } else {
                    nettyOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                }
            });

            channel.writeAndFlush(request, promise);
        } catch (Throwable e) {
            LOG.warn("Operation {} failed", StringUtils.requestToString(request), e);
            errorOut(key);
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

        if (cause instanceof DecoderException && cause.getCause() instanceof SSLHandshakeException) {
            LOG.error("TLS handshake failed", cause);
            errorOutPendingOps(BKException.Code.SecurityException);
            Channel c = ctx.channel();
            if (c != null) {
                closeChannel(c);
            }
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

        CompletionKey key = acquireV2Key(response.ledgerId, response.entryId, operationType);
        CompletionValue completionValue = getCompletionValue(key);
        key.release();

        if (null == completionValue) {
            // Unexpected response, so log it. The txnId should have been present.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected response received from bookie : " + addr + " for type : " + operationType
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

    private static class ReadV2ResponseCallback extends SafeRunnable {
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
        public void safeRun() {
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
                LOG.debug("Unexpected response received from bookie : " + addr + " for type : "
                        + header.getOperation() + " and txnId : " + header.getTxnId());
            }
        } else {
            long orderingKey = completionValue.ledgerId;
            executor.executeOrdered(orderingKey, new SafeRunnable() {
                @Override
                public void safeRun() {
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
                            closeChannel(channel);
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
                        if (conf.getHostnameVerificationEnabled() && !authHandler.verifyTlsHostName(channel)) {
                            // add HostnameVerification or private classes not
                            // for validation
                            rc = BKException.Code.UnauthorizedAccessException;
                        } else {
                                authHandler.authProvider.onProtocolUpgrade();
                                activeTlsChannelCounter.inc();
                            }
                        } else if (future.isSuccess()
                                && (state == ConnectionState.CLOSED || state == ConnectionState.DISCONNECTED)) {
                            LOG.warn("Closed before TLS handshake completed, clean up: {}, current state {}",
                                    channel, state);
                            closeChannel(channel);
                            rc = BKException.Code.BookieHandleNotAvailableException;
                            channel = null;
                        } else if (future.isSuccess() && state == ConnectionState.CONNECTED) {
                            LOG.debug("Already connected with another channel({}), so close the new channel({})",
                                    channel, channel);
                            closeChannel(channel);
                            return; // pendingOps should have been completed when other channel connected
                        } else {
                            LOG.error("TLS handshake failed with bookie: {}/{}, current state {} : ",
                                    channel, addr, state, future.cause());
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

    /**
     * Boiler-plate wrapper classes follow.
     *
     */

    // visible for testing
    abstract class CompletionValue {
        private final OpStatsLogger opLogger;
        private final OpStatsLogger timeoutOpLogger;
        private final String operationName;
        private final Map<String, String> mdcContextMap;
        protected Object ctx;
        protected long ledgerId;
        protected long entryId;
        protected long startTime;

        public CompletionValue(String operationName,
                               Object ctx,
                               long ledgerId, long entryId,
                               OpStatsLogger opLogger,
                               OpStatsLogger timeoutOpLogger) {
            this.operationName = operationName;
            this.ctx = ctx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.startTime = MathUtils.nowInNano();
            this.opLogger = opLogger;
            this.timeoutOpLogger = timeoutOpLogger;
            this.mdcContextMap = preserveMdcForTaskExecution ? MDC.getCopyOfContextMap() : null;
        }

        private long latency() {
            return MathUtils.elapsedNanos(startTime);
        }

        void logOpResult(int rc) {
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

        boolean maybeTimeout() {
            if (MathUtils.elapsedNanos(startTime) >= readEntryTimeoutNanos) {
                timeout();
                return true;
            } else {
                return false;
            }
        }

        void timeout() {
            errorOut(BKException.Code.TimeoutException);
            timeoutOpLogger.registerSuccessfulEvent(latency(),
                                                    TimeUnit.NANOSECONDS);
        }

        protected void logResponse(StatusCode status, Object... extraInfo) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got {} response from bookie:{} rc:{}, {}", operationName, addr, status,
                        Joiner.on(":").join(extraInfo));
            }
        }

        protected int convertStatus(StatusCode status, int defaultStatus) {
            // convert to BKException code
            int rcToRet = statusCodeToExceptionCode(status);
            if (rcToRet == BKException.Code.UNINITIALIZED) {
                LOG.error("{} for failed on bookie {} code {}",
                          operationName, addr, status);
                return defaultStatus;
            } else {
                return rcToRet;
            }
        }

        public void restoreMdcContext() {
            MdcUtils.restoreContext(mdcContextMap);
        }

        public abstract void errorOut();
        public abstract void errorOut(int rc);
        public void setOutstanding() {
            // no-op
        }

        protected void errorOutAndRunCallback(final Runnable callback) {
            executor.executeOrdered(ledgerId,
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
                  writeLacOpLogger, writeLacTimeoutOpLogger);
            this.cb = new WriteLacCallback() {
                    @Override
                    public void writeLacComplete(int rc, long ledgerId,
                                                 BookieSocketAddress addr,
                                                 Object ctx) {
                        logOpResult(rc);
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
            StatusCode status = response.getStatus() == StatusCode.EOK
                ? writeLacResponse.getStatus() : response.getStatus();
            long ledgerId = writeLacResponse.getLedgerId();

            if (LOG.isDebugEnabled()) {
                logResponse(status, "ledger", ledgerId);
            }
            int rc = convertStatus(status, BKException.Code.WriteException);
            cb.writeLacComplete(rc, ledgerId, addr, ctx);
        }
    }

    class ForceLedgerCompletion extends CompletionValue {
        final ForceLedgerCallback cb;

        public ForceLedgerCompletion(final CompletionKey key,
                                  final ForceLedgerCallback originalCallback,
                                  final Object originalCtx,
                                  final long ledgerId) {
            super("ForceLedger",
                  originalCtx, ledgerId, BookieProtocol.LAST_ADD_CONFIRMED,
                  forceLedgerOpLogger, forceLedgerTimeoutOpLogger);
            this.cb = new ForceLedgerCallback() {
                    @Override
                    public void forceLedgerComplete(int rc, long ledgerId,
                                                 BookieSocketAddress addr,
                                                 Object ctx) {
                        logOpResult(rc);
                        originalCallback.forceLedgerComplete(rc, ledgerId,
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
                    () -> cb.forceLedgerComplete(rc, ledgerId, addr, ctx));
        }

        @Override
        public void handleV3Response(BookkeeperProtocol.Response response) {
            ForceLedgerResponse forceLedgerResponse = response.getForceLedgerResponse();
            StatusCode status = response.getStatus() == StatusCode.EOK
                ? forceLedgerResponse.getStatus() : response.getStatus();
            long ledgerId = forceLedgerResponse.getLedgerId();

            if (LOG.isDebugEnabled()) {
                logResponse(status, "ledger", ledgerId);
            }
            int rc = convertStatus(status, BKException.Code.WriteException);
            cb.forceLedgerComplete(rc, ledgerId, addr, ctx);
        }
    }

    // visible for testing
    class ReadLacCompletion extends CompletionValue {
        final ReadLacCallback cb;

        public ReadLacCompletion(final CompletionKey key,
                                 ReadLacCallback originalCallback,
                                 final Object ctx, final long ledgerId) {
            super("ReadLAC", ctx, ledgerId, BookieProtocol.LAST_ADD_CONFIRMED,
                  readLacOpLogger, readLacTimeoutOpLogger);
            this.cb = new ReadLacCallback() {
                    @Override
                    public void readLacComplete(int rc, long ledgerId,
                                                ByteBuf lacBuffer,
                                                ByteBuf lastEntryBuffer,
                                                Object ctx) {
                        logOpResult(rc);
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
            StatusCode status = response.getStatus() == StatusCode.EOK
                ? readLacResponse.getStatus() : response.getStatus();

            if (readLacResponse.hasLacBody()) {
                lacBuffer = Unpooled.wrappedBuffer(readLacResponse.getLacBody().asReadOnlyByteBuffer());
            }

            if (readLacResponse.hasLastEntryBody()) {
                lastEntryBuffer = Unpooled.wrappedBuffer(readLacResponse.getLastEntryBody().asReadOnlyByteBuffer());
            }

            if (LOG.isDebugEnabled()) {
                logResponse(status, "ledgerId", ledgerId);
            }

            int rc = convertStatus(status, BKException.Code.ReadException);
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
                  readEntryOpLogger, readTimeoutOpLogger);

            this.cb = new ReadEntryCallback() {
                    @Override
                    public void readEntryComplete(int rc, long ledgerId,
                                                  long entryId, ByteBuf buffer,
                                                  Object ctx) {
                        logOpResult(rc);
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
        public void setOutstanding() {
            readEntryOutstanding.inc();
        }

        @Override
        public void handleV2Response(long ledgerId, long entryId,
                                     StatusCode status,
                                     BookieProtocol.Response response) {
            readEntryOutstanding.dec();
            if (!(response instanceof BookieProtocol.ReadResponse)) {
                return;
            }
            BookieProtocol.ReadResponse readResponse = (BookieProtocol.ReadResponse) response;
            handleReadResponse(ledgerId, entryId, status, readResponse.getData(),
                               INVALID_ENTRY_ID, -1L);
        }

        @Override
        public void handleV3Response(BookkeeperProtocol.Response response) {
            readEntryOutstanding.dec();
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
            buffer.release(); // meaningless using unpooled, but client may expect to hold the last reference
        }

        private void handleReadResponse(long ledgerId,
                                        long entryId,
                                        StatusCode status,
                                        ByteBuf buffer,
                                        long maxLAC, // max known lac piggy-back from bookies
                                        long lacUpdateTimestamp) { // the timestamp when the lac is updated.
            int readableBytes = buffer.readableBytes();
            if (LOG.isDebugEnabled()) {
                logResponse(status, "ledger", ledgerId, "entry", entryId, "entryLength", readableBytes);
            }

            int rc = convertStatus(status, BKException.Code.ReadException);

            if (maxLAC > INVALID_ENTRY_ID && (ctx instanceof ReadEntryCallbackCtx)) {
                ((ReadEntryCallbackCtx) ctx).setLastAddConfirmed(maxLAC);
            }
            if (lacUpdateTimestamp > -1L && (ctx instanceof ReadLastConfirmedAndEntryContext)) {
                ((ReadLastConfirmedAndEntryContext) ctx).setLacUpdateTimestamp(lacUpdateTimestamp);
            }
            cb.readEntryComplete(rc, ledgerId, entryId, buffer.slice(), ctx);
        }
    }

    class StartTLSCompletion extends CompletionValue {
        final StartTLSCallback cb;

        public StartTLSCompletion(final CompletionKey key) {
            super("StartTLS", null, -1, -1,
                  startTLSOpLogger, startTLSTimeoutOpLogger);
            this.cb = new StartTLSCallback() {
                @Override
                public void startTLSComplete(int rc, Object ctx) {
                    logOpResult(rc);
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

            if (LOG.isDebugEnabled()) {
                logResponse(status);
            }

            int rc = convertStatus(status, BKException.Code.SecurityException);

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
                  getBookieInfoOpLogger, getBookieInfoTimeoutOpLogger);
            this.cb = new GetBookieInfoCallback() {
                @Override
                public void getBookieInfoComplete(int rc, BookieInfo bInfo,
                                                  Object ctx) {
                    logOpResult(rc);
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
            GetBookieInfoResponse getBookieInfoResponse = response.getGetBookieInfoResponse();
            StatusCode status = response.getStatus() == StatusCode.EOK
                ? getBookieInfoResponse.getStatus() : response.getStatus();

            long freeDiskSpace = getBookieInfoResponse.getFreeDiskSpace();
            long totalDiskSpace = getBookieInfoResponse.getTotalDiskCapacity();

            if (LOG.isDebugEnabled()) {
                logResponse(status, "freeDisk", freeDiskSpace, "totalDisk", totalDiskSpace);
            }

            int rc = convertStatus(status, BKException.Code.ReadException);
            cb.getBookieInfoComplete(rc,
                                     new BookieInfo(totalDiskSpace,
                                                    freeDiskSpace), ctx);
        }
    }

    class GetListOfEntriesOfLedgerCompletion extends CompletionValue {
        final GetListOfEntriesOfLedgerCallback cb;

        public GetListOfEntriesOfLedgerCompletion(final CompletionKey key,
                final GetListOfEntriesOfLedgerCallback origCallback, final long ledgerId) {
            super("GetListOfEntriesOfLedger", null, ledgerId, 0L, getListOfEntriesOfLedgerCompletionOpLogger,
                    getListOfEntriesOfLedgerCompletionTimeoutOpLogger);
            this.cb = new GetListOfEntriesOfLedgerCallback() {
                @Override
                public void getListOfEntriesOfLedgerComplete(int rc, long ledgerId,
                        AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger) {
                    logOpResult(rc);
                    origCallback.getListOfEntriesOfLedgerComplete(rc, ledgerId, availabilityOfEntriesOfLedger);
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
            errorOutAndRunCallback(() -> cb.getListOfEntriesOfLedgerComplete(rc, ledgerId, null));
        }

        @Override
        public void handleV3Response(BookkeeperProtocol.Response response) {
            GetListOfEntriesOfLedgerResponse getListOfEntriesOfLedgerResponse = response
                    .getGetListOfEntriesOfLedgerResponse();
            ByteBuf availabilityOfEntriesOfLedgerBuffer = Unpooled.EMPTY_BUFFER;
            StatusCode status = response.getStatus() == StatusCode.EOK ? getListOfEntriesOfLedgerResponse.getStatus()
                    : response.getStatus();

            if (getListOfEntriesOfLedgerResponse.hasAvailabilityOfEntriesOfLedger()) {
                availabilityOfEntriesOfLedgerBuffer = Unpooled.wrappedBuffer(
                        getListOfEntriesOfLedgerResponse.getAvailabilityOfEntriesOfLedger().asReadOnlyByteBuffer());
            }

            if (LOG.isDebugEnabled()) {
                logResponse(status, "ledgerId", ledgerId);
            }

            int rc = convertStatus(status, BKException.Code.ReadException);
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = null;
            if (rc == BKException.Code.OK) {
                availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                        availabilityOfEntriesOfLedgerBuffer.slice());
            }
            cb.getListOfEntriesOfLedgerComplete(rc, ledgerId, availabilityOfEntriesOfLedger);
        }
    }

    private final Recycler<AddCompletion> addCompletionRecycler = new Recycler<AddCompletion>() {
            @Override
            protected AddCompletion newObject(Recycler.Handle<AddCompletion> handle) {
                return new AddCompletion(handle);
            }
        };

    AddCompletion acquireAddCompletion(final CompletionKey key,
                                       final WriteCallback originalCallback,
                                       final Object originalCtx,
                                       final long ledgerId, final long entryId) {
        AddCompletion completion = addCompletionRecycler.get();
        completion.reset(key, originalCallback, originalCtx, ledgerId, entryId);
        return completion;
    }

    // visible for testing
    class AddCompletion extends CompletionValue implements WriteCallback {
        final Recycler.Handle<AddCompletion> handle;

        CompletionKey key = null;
        WriteCallback originalCallback = null;

        AddCompletion(Recycler.Handle<AddCompletion> handle) {
            super("Add", null, -1, -1, addEntryOpLogger, addTimeoutOpLogger);
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
        }

        @Override
        public void writeComplete(int rc, long ledgerId, long entryId,
                                  BookieSocketAddress addr,
                                  Object ctx) {
            logOpResult(rc);
            originalCallback.writeComplete(rc, ledgerId, entryId, addr, ctx);
            key.release();
            handle.recycle(this);
        }

        @Override
        boolean maybeTimeout() {
            if (MathUtils.elapsedNanos(startTime) >= addEntryTimeoutNanos) {
                timeout();
                return true;
            } else {
                return false;
            }
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
        public void setOutstanding() {
            addEntryOutstanding.inc();
        }

        @Override
        public void handleV2Response(
                long ledgerId, long entryId, StatusCode status,
                BookieProtocol.Response response) {
            addEntryOutstanding.dec();
            handleResponse(ledgerId, entryId, status);
        }

        @Override
        public void handleV3Response(
                BookkeeperProtocol.Response response) {
            addEntryOutstanding.dec();
            AddResponse addResponse = response.getAddResponse();
            StatusCode status = response.getStatus() == StatusCode.EOK
                ? addResponse.getStatus() : response.getStatus();
            handleResponse(addResponse.getLedgerId(), addResponse.getEntryId(),
                           status);
        }

        private void handleResponse(long ledgerId, long entryId,
                                    StatusCode status) {
            if (LOG.isDebugEnabled()) {
                logResponse(status, "ledger", ledgerId, "entry", entryId);
            }

            int rc = convertStatus(status, BKException.Code.WriteException);
            writeComplete(rc, ledgerId, entryId, addr, ctx);
        }
    }

    // visable for testing
    CompletionKey newCompletionKey(long txnId, OperationType operationType) {
        return new V3CompletionKey(txnId, operationType);
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

    abstract class CompletionKey {
        final long txnId;
        OperationType operationType;

        CompletionKey(long txnId,
                      OperationType operationType) {
            this.txnId = txnId;
            this.operationType = operationType;
        }

        public void release() {}
    }

    /**
     * Note : Helper functions follow
     */

    /**
     * @param status
     * @return {@link BKException.Code.UNINITIALIZED} if the statuscode is unknown.
     */
    private int statusCodeToExceptionCode(StatusCode status) {
        switch (status) {
            case EOK:
                return BKException.Code.OK;
            case ENOENTRY:
                return BKException.Code.NoSuchEntryException;
            case ENOLEDGER:
                return BKException.Code.NoSuchLedgerExistsException;
            case EBADVERSION:
                return BKException.Code.ProtocolVersionException;
            case EUA:
                return BKException.Code.UnauthorizedAccessException;
            case EFENCED:
                return BKException.Code.LedgerFencedException;
            case EREADONLY:
                return BKException.Code.WriteOnReadOnlyBookieException;
            case ETOOMANYREQUESTS:
                return BKException.Code.TooManyRequestsException;
            default:
                return BKException.Code.UNINITIALIZED;
        }
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

    private final Recycler<V2CompletionKey> v2KeyRecycler = new Recycler<V2CompletionKey>() {
            @Override
            protected V2CompletionKey newObject(
                    Recycler.Handle<V2CompletionKey> handle) {
                return new V2CompletionKey(handle);
            }
        };

    V2CompletionKey acquireV2Key(long ledgerId, long entryId,
                             OperationType operationType) {
        V2CompletionKey key = v2KeyRecycler.get();
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
        public void operationComplete(ChannelFuture future) throws Exception {
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
                    LOG.info("Successfully connected to bookie: {}", future.channel());
                    rc = BKException.Code.OK;
                    channel = future.channel();
                    if (shFactory != null) {
                        makeWritable();
                        initiateTLS();
                        return;
                    } else {
                        LOG.info("Successfully connected to bookie: " + addr);
                        state = ConnectionState.CONNECTED;
                        activeNonTlsChannelCounter.inc();
                    }
                } else if (future.isSuccess() && state == ConnectionState.START_TLS) {
                    rc = BKException.Code.OK;
                    LOG.info("Successfully connected to bookie using TLS: " + addr);

                    state = ConnectionState.CONNECTED;
                    AuthHandler.ClientSideHandler authHandler = future.channel().pipeline()
                            .get(AuthHandler.ClientSideHandler.class);
                    if (conf.getHostnameVerificationEnabled() && !authHandler.verifyTlsHostName(channel)) {
                        rc = BKException.Code.UnauthorizedAccessException;
                    } else {
                        authHandler.authProvider.onProtocolUpgrade();
                        activeTlsChannelCounter.inc();
                    }
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
                        LOG.warn("Could not connect to bookie: {}/{}, current state {} : {}",
                                future.channel(), addr, state, future.cause().getMessage());
                    } else {
                        // Regular exceptions, include stack trace
                        LOG.error("Could not connect to bookie: {}/{}, current state {} : ",
                                future.channel(), addr, state, future.cause());
                    }

                    rc = BKException.Code.BookieHandleNotAvailableException;
                    closeChannel(future.channel());
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
    }

    private void initiateTLS() {
        LOG.info("Initializing TLS to {}", channel);
        assert state == ConnectionState.CONNECTING;
        final long txnId = getTxnId();
        final CompletionKey completionKey = new V3CompletionKey(txnId, OperationType.START_TLS);
        completionObjects.put(completionKey,
                              new StartTLSCompletion(completionKey));
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

    private void failTLS(int rc) {
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
}
