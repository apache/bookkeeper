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

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
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
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
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
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.LocalClientChannelFactory;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.CorruptedFrameException;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import java.net.SocketAddress;
import java.util.Collection;
import org.apache.bookkeeper.auth.BookKeeperPrincipal;
import org.jboss.netty.channel.ChannelFactory;
import org.apache.bookkeeper.client.ClientConnectionPeer;

/**
 * This class manages all details of connection to a particular bookie. It also
 * has reconnect logic if a connection to a bookie fails.
 *
 */
public class PerChannelBookieClient extends SimpleChannelHandler implements ChannelPipelineFactory {

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
    final ChannelFactory channelFactory;
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

    public PerChannelBookieClient(OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                                  BookieSocketAddress addr) {
        this(new ClientConfiguration(), executor, channelFactory, addr, null, NullStatsLogger.INSTANCE, null, null, null);
    }

    public PerChannelBookieClient(OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                                  BookieSocketAddress addr,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry) {
        this(new ClientConfiguration(), executor, channelFactory, addr, null, NullStatsLogger.INSTANCE,
                authProviderFactory, extRegistry, null);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedSafeExecutor executor,
                                  ClientSocketChannelFactory channelFactory, BookieSocketAddress addr,
                                  HashedWheelTimer requestTimer, StatsLogger parentStatsLogger,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry,
                                  PerChannelBookieClientPool pcbcPool) {
        this.maxFrameSize = conf.getNettyMaxFrameSizeBytes();
        this.conf = conf;
        this.addr = addr;
        this.executor = executor;
        if (LocalBookiesRegistry.isLocalBookie(addr)){
            this.channelFactory = new DefaultLocalClientChannelFactory();
        } else {
            this.channelFactory = channelFactory;
        }
        this.state = ConnectionState.DISCONNECTED;
        this.requestTimer = requestTimer;
        this.addEntryTimeout = conf.getAddEntryTimeout();
        this.readEntryTimeout = conf.getReadEntryTimeout();
        this.getBookieInfoTimeout = conf.getBookieInfoTimeout();

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
                    return c.getRemoteAddress();
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

    private void connect() {
        LOG.debug("Connecting to bookie: {}", addr);

        // Set up the ClientBootStrap so we can create a new Channel connection
        // to the bookie.
        ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
        bootstrap.setPipelineFactory(this);
        bootstrap.setOption("tcpNoDelay", conf.getClientTcpNoDelay());
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("connectTimeoutMillis", conf.getClientConnectTimeoutMillis());
        bootstrap.setOption("child.sendBufferSize", conf.getClientSendBufferSize());
        bootstrap.setOption("child.receiveBufferSize", conf.getClientReceiveBufferSize());
        bootstrap.setOption("writeBufferLowWaterMark", conf.getClientWriteBufferLowWaterMark());
        bootstrap.setOption("writeBufferHighWaterMark", conf.getClientWriteBufferHighWaterMark());
        SocketAddress bookieAddr = addr.getSocketAddress();        
        if (channelFactory instanceof LocalClientChannelFactory) {
            bookieAddr = addr.getLocalAddress();
        }
        ChannelFuture future = bootstrap.connect(bookieAddr);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOG.debug("Channel connected ({}) {}", future.isSuccess(), future.getChannel());
                int rc;
                Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;

                synchronized (PerChannelBookieClient.this) {
                    if (future.isSuccess() && state == ConnectionState.CONNECTING) {
                        LOG.info("Successfully connected to bookie: {}", future.getChannel());
                        rc = BKException.Code.OK;
                        channel = future.getChannel();
                        state = ConnectionState.CONNECTED;
                    } else if (future.isSuccess() && (state == ConnectionState.CLOSED
                                                      || state == ConnectionState.DISCONNECTED)) {
                        LOG.warn("Closed before connection completed, clean up: {}, current state {}",
                                 future.getChannel(), state);
                        closeChannel(future.getChannel());
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        channel = null;
                    } else if (future.isSuccess() && state == ConnectionState.CONNECTED) {
                        LOG.debug("Already connected with another channel({}), so close the new channel({})",
                                  channel, future.getChannel());
                        closeChannel(future.getChannel());
                        return; // pendingOps should have been completed when other channel connected
                    } else {
                        LOG.error("Could not connect to bookie: {}/{}, current state {} : ",
                                  new Object[] { future.getChannel(), addr,
                                                 state, future.getCause() });
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        closeChannel(future.getChannel());
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

    void writeLac(final long ledgerId, final byte[] masterKey, final long lac, ChannelBuffer toSend, WriteLacCallback cb, Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.WRITE_LAC);
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
                .setBody(ByteString.copyFrom(toSend.toByteBuffer()));

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
            ChannelFuture future = c.write(writeLacRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request for writeLac LedgerId: {} bookie: {}",
                                    ledgerId, c.getRemoteAddress());
                        }
                    } else {
                        if (!(future.getCause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing Lac(lid={} to channel {} failed : ",
                                    new Object[] { ledgerId, c, future.getCause() });
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
    void addEntry(final long ledgerId, byte[] masterKey, final long entryId, ChannelBuffer toSend, WriteCallback cb,
                  Object ctx, final int options) {
        final long txnId = getTxnId();
        final int entrySize = toSend.readableBytes();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.ADD_ENTRY);
        completionObjects.put(completionKey,
                new AddCompletion(this, addEntryOpLogger, cb, ctx, ledgerId, entryId,
                                  scheduleTimeout(completionKey, addEntryTimeout)));

        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(txnId);

        AddRequest.Builder addBuilder = AddRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setMasterKey(ByteString.copyFrom(masterKey))
                .setBody(ByteString.copyFrom(toSend.toByteBuffer()));

        if (((short)options & BookieProtocol.FLAG_RECOVERY_ADD) == BookieProtocol.FLAG_RECOVERY_ADD) {
            addBuilder.setFlag(AddRequest.Flag.RECOVERY_ADD);
        }

        final Request addRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setAddRequest(addBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutAddKey(completionKey);
            return;
        }
        try {
            ChannelFuture future = c.write(addRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request for adding entry: " + entryId + " ledger-id: " + ledgerId
                                                            + " bookie: " + c.getRemoteAddress() + " entry length: " + entrySize);
                        }
                        // totalBytesOutstanding.addAndGet(entrySize);
                    } else {
                        if (!(future.getCause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing addEntry(lid={}, eid={}) to channel {} failed : ",
                                    new Object[] { ledgerId, entryId, c, future.getCause() });
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
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.READ_ENTRY);
        completionObjects.put(completionKey,
                new ReadCompletion(this, readEntryOpLogger, cb, ctx, ledgerId, entryId,
                                   scheduleTimeout(completionKey, readEntryTimeout)));

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

        final Request readRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setReadRequest(readBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(completionKey);
            return;
        }

        try {
            ChannelFuture future = c.write(readRequest);
            future.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Successfully wrote request {} to {}",
                                          readRequest, c.getRemoteAddress());
                            }
                        } else {
                            if (!(future.getCause() instanceof ClosedChannelException)) {
                                LOG.warn("Writing readEntryAndFenceLedger(lid={}, eid={}) to channel {} failed : ",
                                        new Object[] { ledgerId, entryId, c, future.getCause() });
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
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.READ_LAC);
        completionObjects.put(completionKey,
                new ReadLacCompletion(readLacOpLogger, cb, ctx, ledgerId,
                        scheduleTimeout(completionKey, readEntryTimeout)));
        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.READ_LAC)
                .setTxnId(txnId);
        ReadLacRequest.Builder readLacBuilder = ReadLacRequest.newBuilder()
                .setLedgerId(ledgerId);
        final Request readLacRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setReadLacRequest(readLacBuilder)
                .build();
        final Channel c = channel;
        if (c == null) {
            errorOutReadLacKey(completionKey);
            return;
        }

        try {
            ChannelFuture future = c.write(readLacRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        LOG.debug("Succssfully wrote request {} to {}",
                                    readLacRequest, c.getRemoteAddress());
                    } else {
                        if (!(future.getCause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing readLac(lid = {}) to channel {} failed : ",
                                    new Object[] { ledgerId, c, future.getCause() });
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
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.READ_ENTRY);
        completionObjects.put(completionKey,
                new ReadCompletion(this, readEntryOpLogger, cb, ctx, ledgerId, entryId,
                                   scheduleTimeout(completionKey, readEntryTimeout)));

        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.READ_ENTRY)
                .setTxnId(txnId);

        ReadRequest.Builder readBuilder = ReadRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        final Request readRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setReadRequest(readBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(completionKey);
            return;
        }

        try{
            ChannelFuture future = c.write(readRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request {} to {}",
                                      readRequest, c.getRemoteAddress());
                        }
                    } else {
                        if (!(future.getCause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing readEntry(lid={}, eid={}) to channel {} failed : ",
                                    new Object[] { ledgerId, entryId, c, future.getCause() });
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
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.GET_BOOKIE_INFO);
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
            ChannelFuture future = c.write(getBookieInfoRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request {} to {}",
                                    getBookieInfoRequest, c.getRemoteAddress());
                        }
                    } else {
                        if (!(future.getCause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing GetBookieInfoRequest(flags={}) to channel {} failed : ",
                                    new Object[] { requested, c, future.getCause() });
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
        LOG.debug("Closing channel {}", c);
        ReadTimeoutHandler timeout = c.getPipeline().get(ReadTimeoutHandler.class);
        if (timeout != null) {
            timeout.releaseExternalResources();
        }
        return c.close();
    }

    void errorOutReadKey(final CompletionKey key) {
        errorOutReadKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutReadKey(final CompletionKey key, final int rc) {
        final ReadCompletion readCompletion = (ReadCompletion)completionObjects.remove(key);
        if (null == readCompletion) {
            return;
        }
        executor.submitOrdered(readCompletion.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if (c != null && c.getRemoteAddress() != null) {
                    bAddress = c.getRemoteAddress().toString();
                }

                LOG.debug("Could not write request for reading entry: {} ledger-id: {} bookie: {} rc: {}",
                        new Object[]{ readCompletion.entryId, readCompletion.ledgerId, bAddress, rc });

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
                    bAddress = c.getRemoteAddress().toString();
                }
                LOG.debug("Could not write request writeLac for ledgerId: {} bookie: {}",
                          new Object[] { writeLacCompletion.ledgerId, bAddress});
                writeLacCompletion.cb.writeLacComplete(rc, writeLacCompletion.ledgerId, addr, writeLacCompletion.ctx);
            }
        });
    }

    void errorOutReadLacKey(final CompletionKey key) {
        errorOutReadLacKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutReadLacKey(final CompletionKey key, final int rc) {
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
                    bAddress = c.getRemoteAddress().toString();
                }
                LOG.debug("Could not write request readLac for ledgerId: {} bookie: {}",
                          new Object[] { readLacCompletion.ledgerId, bAddress});
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
                if(c != null && c.getRemoteAddress() != null) {
                    bAddress = c.getRemoteAddress().toString();
                }
                LOG.debug("Could not write request for adding entry: {} ledger-id: {} bookie: {} rc: {}",
                          new Object[] { addCompletion.entryId, addCompletion.ledgerId, bAddress, rc });

                addCompletion.cb.writeComplete(rc, addCompletion.ledgerId, addCompletion.entryId,
                                               addr, addCompletion.ctx);
                LOG.debug("Invoked callback method: {}", addCompletion.entryId);
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
                    bAddress = c.getRemoteAddress().toString();
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
     * In the netty pipeline, we need to split packets based on length, so we
     * use the {@link LengthFieldBasedFrameDecoder}. Other than that all actions
     * are carried out in this class, e.g., making sense of received messages,
     * prepending the length to outgoing packets etc.
     */
    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();

        pipeline.addLast("lengthbasedframedecoder", new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
        pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
        pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.RequestEncoder(extRegistry));
        pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.ResponseDecoder(extRegistry));
        pipeline.addLast("authHandler", new AuthHandler.ClientSideHandler(authProviderFactory, txnIdGenerator,
            connectionPeer));
        pipeline.addLast("mainhandler", this);
        return pipeline;
    }

    /**
     * If our channel has disconnected, we just error out the pending entries
     */
    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        Channel c = ctx.getChannel();
        LOG.info("Disconnected from bookie channel {}", c);
        if (c != null) {
            closeChannel(c);
        }

        errorOutOutstandingEntries(BKException.Code.BookieHandleNotAvailableException);

        synchronized (this) {
            if (this.channel == c
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
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Throwable t = e.getCause();
        if (t instanceof CorruptedFrameException || t instanceof TooLongFrameException) {
            LOG.error("Corrupted frame received from bookie: {}",
                      e.getChannel().getRemoteAddress());
            return;
        }

        if (t instanceof AuthHandler.AuthenticationException) {
            LOG.error("Error authenticating connection", t);
            errorOutOutstandingEntries(BKException.Code.UnauthorizedAccessException);
            Channel c = ctx.getChannel();
            if (c != null) {
                closeChannel(c);
            }
            return;
        }

        if (t instanceof IOException) {
            // these are thrown when a bookie fails, logging them just pollutes
            // the logs (the failure is logged from the listeners on the write
            // operation), so I'll just ignore it here.
            return;
        }

        synchronized (this) {
            if (state == ConnectionState.CLOSED) {
                LOG.debug("Unexpected exception caught by bookie client channel handler, "
                          + "but the client is closed, so it isn't important", t);
            } else {
                LOG.error("Unexpected exception caught by bookie client channel handler", t);
            }
        }
        // Since we are a library, cant terminate App here, can we?
    }

    /**
     * Called by netty when a message is received on a channel
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if (!(e.getMessage() instanceof Response)) {
            ctx.sendUpstream(e);
            return;
        }

        final Response response = (Response) e.getMessage();
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
                        case ADD_ENTRY:
                            handleAddResponse(response, completionValue);
                            break;
                        case READ_ENTRY:
                            handleReadResponse(response, completionValue);
                            break;
                        case WRITE_LAC:
                            handleWriteLacResponse(response.getWriteLacResponse(), completionValue);
                            break;
                        case READ_LAC:
                            handleReadLacResponse(response.getReadLacResponse(), completionValue);
                            break;
                        case GET_BOOKIE_INFO:
                            handleGetBookieInfoResponse(response, completionValue);
                            break;
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

    void handleWriteLacResponse(WriteLacResponse writeLacResponse, CompletionValue completionValue) {
        // The completion value should always be an instance of an WriteLacCompletion object when we reach here.
        WriteLacCompletion plc = (WriteLacCompletion)completionValue;

        long ledgerId = writeLacResponse.getLedgerId();
        StatusCode status = writeLacResponse.getStatus();

        LOG.debug("Got response for writeLac request from bookie: " + addr + " for ledger: " + ledgerId + " rc: " + status);

        // convert to BKException code
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.error("writeLac for ledger: " + ledgerId + " failed on bookie: " + addr
                        + " with code:" + status);
            rcToRet = BKException.Code.WriteException;
        }
        plc.cb.writeLacComplete(rcToRet, ledgerId, addr, plc.ctx);
    }

 void handleAddResponse(Response response, CompletionValue completionValue) {
        // The completion value should always be an instance of an AddCompletion object when we reach here.
        AddCompletion ac = (AddCompletion)completionValue;
        AddResponse addResponse = response.getAddResponse();

        long ledgerId = addResponse.getLedgerId();
        long entryId = addResponse.getEntryId();
        StatusCode status = response.getStatus() == StatusCode.EOK ? addResponse.getStatus() : response.getStatus();

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

    void handleReadLacResponse(ReadLacResponse readLacResponse, CompletionValue completionValue) {
        // The completion value should always be an instance of an WriteLacCompletion object when we reach here.
        ReadLacCompletion glac = (ReadLacCompletion)completionValue;

        long ledgerId = readLacResponse.getLedgerId();
        StatusCode status = readLacResponse.getStatus();
        ChannelBuffer lacBuffer = ChannelBuffers.buffer(0);
        ChannelBuffer lastEntryBuffer = ChannelBuffers.buffer(0);

       // Thread.dumpStack();

        if (readLacResponse.hasLacBody()) {
            lacBuffer = ChannelBuffers.copiedBuffer(readLacResponse.getLacBody().asReadOnlyByteBuffer());
        }

        if (readLacResponse.hasLastEntryBody()) {
            lastEntryBuffer = ChannelBuffers.copiedBuffer(readLacResponse.getLastEntryBody().asReadOnlyByteBuffer());
        }

        LOG.debug("Got response for readLac request from bookie: " + addr + " for ledger: " + ledgerId + " rc: " + status);
        // convert to BKException code
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.debug("readLac for ledger: " + ledgerId + " failed on bookie: " + addr
                      + " with code:" + status);
            rcToRet = BKException.Code.ReadException;
        }
        glac.cb.readLacComplete(rcToRet, ledgerId, lacBuffer.slice(), lastEntryBuffer.slice(), glac.ctx);
    }

    void handleReadResponse(Response response, CompletionValue completionValue) {
        // The completion value should always be an instance of a ReadCompletion object when we reach here.
        ReadCompletion rc = (ReadCompletion)completionValue;
        ReadResponse readResponse = response.getReadResponse();

        long ledgerId = readResponse.getLedgerId();
        long entryId = readResponse.getEntryId();
        StatusCode status = response.getStatus() == StatusCode.EOK ? readResponse.getStatus() : response.getStatus();

        ChannelBuffer buffer = ChannelBuffers.buffer(0);

        if (readResponse.hasBody()) {
            buffer = ChannelBuffers.copiedBuffer(readResponse.getBody().asReadOnlyByteBuffer());
        }

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
        rc.cb.readEntryComplete(rcToRet, ledgerId, entryId, buffer.slice(), rc.ctx);
    }

    void handleGetBookieInfoResponse(Response response, CompletionValue completionValue) {
        // The completion value should always be an instance of a GetBookieInfoCompletion object when we reach here.
        GetBookieInfoCompletion rc = (GetBookieInfoCompletion)completionValue;
        GetBookieInfoResponse getBookieInfoResponse = response.getGetBookieInfoResponse();

        long freeDiskSpace = getBookieInfoResponse.hasFreeDiskSpace() ? getBookieInfoResponse.getFreeDiskSpace() : 0L;
        long totalDiskCapacity = getBookieInfoResponse.hasTotalDiskCapacity() ? getBookieInfoResponse.getTotalDiskCapacity() : 0L;

        StatusCode status = response.getStatus() == StatusCode.EOK ? getBookieInfoResponse.getStatus() : response.getStatus();

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
        LOG.debug("Response received from bookie info read: freeDiskSpace=" +  freeDiskSpace + " totalDiskSpace:" + totalDiskCapacity);
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
                public void readLacComplete(int rc, long ledgerId, ChannelBuffer lacBuffer, ChannelBuffer lastEntryBuffer, Object ctx) {
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
                public void readEntryComplete(int rc, long ledgerId, long entryId, ChannelBuffer buffer, Object ctx) {
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
        return new CompletionKey(txnId, operationType);
    }

    Timeout scheduleTimeout(CompletionKey key, long timeout) {
        if (null != requestTimer) {
            return requestTimer.newTimeout(key, timeout, TimeUnit.SECONDS);
        } else {
            return null;
        }
    }

    class CompletionKey implements TimerTask {
        final long txnId;
        final OperationType operationType;
        final long requestAt;

        CompletionKey(long txnId, OperationType operationType) {
            this.txnId = txnId;
            this.operationType = operationType;
            this.requestAt = MathUtils.nowInNano();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CompletionKey)) {
                return false;
            }
            CompletionKey that = (CompletionKey) obj;
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
            default:
                break;
        }
        return rcToRet;
    }

    private long getTxnId() {
        return txnIdGenerator.incrementAndGet();
    }

}
