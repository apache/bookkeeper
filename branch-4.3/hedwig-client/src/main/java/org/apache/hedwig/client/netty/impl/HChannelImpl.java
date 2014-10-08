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
package org.apache.hedwig.client.netty.impl;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.Queue;

import com.google.protobuf.ByteString;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.exceptions.NoResponseHandlerException;
import org.apache.hedwig.client.netty.HChannel;
import org.apache.hedwig.client.netty.NetUtils;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.util.HedwigSocketAddress;
import static org.apache.hedwig.util.VarArgs.va;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide a wrapper over netty channel for Hedwig operations.
 */
public class HChannelImpl implements HChannel {

    private static Logger logger = LoggerFactory.getLogger(HChannelImpl.class);

    enum State {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
    };

    InetSocketAddress host;
    final AbstractHChannelManager channelManager;
    final ClientChannelPipelineFactory pipelineFactory;
    volatile Channel channel;
    volatile State state;

    // Indicates whether the channel is closed or not.
    volatile boolean closed = false;
    // Queue the pubsub requests when the channel is not connected.
    Queue<PubSubData> pendingOps = new ArrayDeque<PubSubData>();

    /**
     * Create a un-established channel with provided target <code>host</code>.
     *
     * @param host
     *          Target host address.
     * @param channelManager
     *          Channel manager manages the channels.
     */
    protected HChannelImpl(InetSocketAddress host, AbstractHChannelManager channelManager) {
        this(host, channelManager, null);
    }

    public HChannelImpl(InetSocketAddress host, AbstractHChannelManager channelManager,
                        ClientChannelPipelineFactory pipelineFactory) {
        this(host, null, channelManager, pipelineFactory);
        state = State.DISCONNECTED;
    }

    /**
     * Create a <code>HChannel</code> with an established netty channel.
     *
     * @param host
     *          Target host address.
     * @param channel
     *          Established Netty channel.
     * @param channelManager
     *          Channel manager manages the channels.
     */
    public HChannelImpl(InetSocketAddress host, Channel channel,
                        AbstractHChannelManager channelManager,
                        ClientChannelPipelineFactory pipelineFactory) {
        this.host = host;
        this.channel = channel;
        this.channelManager = channelManager;
        this.pipelineFactory = pipelineFactory;
        state = State.CONNECTED;
    }

    @Override
    public void submitOp(PubSubData pubSubData) {
        boolean doOpNow = false;

        // common case without lock first
        if (null != channel && State.CONNECTED == state) {
            doOpNow = true;
        } else {
            synchronized (this) {
                // check channel & state again under lock
                if (null != channel && State.CONNECTED == state) {
                    doOpNow = true;
                } else {
                    // if reached here, channel is either null (first connection attempt),
                    // or the channel is disconnected. Connection attempt is still in progress,
                    // queue up this op. Op will be executed when connection attempt either
                    // fails or succeeds
                    pendingOps.add(pubSubData);
                }
            }
            if (!doOpNow) {
                // start connection attempt to server
                connect();
            }
        }
        if (doOpNow) {
            executeOpAfterConnected(pubSubData); 
        }
    }

    /**
     * Execute pub/sub operation after the underlying channel is connected.
     *
     * @param pubSubData
     *          Pub/Sub Operation
     */
    private void executeOpAfterConnected(PubSubData pubSubData) {
        PubSubRequest.Builder reqBuilder =
            NetUtils.buildPubSubRequest(channelManager.nextTxnId(), pubSubData);
        writePubSubRequest(pubSubData, reqBuilder.build());
    }

    @Override
    public Channel getChannel() {
        return channel;
    }

    private void writePubSubRequest(PubSubData pubSubData, PubSubRequest pubSubRequest) {
        if (closed || null == channel || State.CONNECTED != state) {
            retryOrFailOp(pubSubData);
            return;
        }

        // Before we do the write, store this information into the
        // ResponseHandler so when the server responds, we know what
        // appropriate Callback Data to invoke for the given txn ID.
        try {
            getHChannelHandlerFromChannel(channel)
                .addTxn(pubSubData.txnId, pubSubData);
        } catch (NoResponseHandlerException nrhe) {
            logger.warn("No Channel Handler found for channel {} when writing request."
                        + " It might already disconnect.", channel);
            return;
        }

        // Finally, write the pub/sub request through the Channel.
        logger.debug("Writing a {} request to host: {} for pubSubData: {}.",
                     va(pubSubData.operationType, host, pubSubData));
        ChannelFuture future = channel.write(pubSubRequest);
        future.addListener(new WriteCallback(pubSubData, channelManager));
    }

    /**
     * Re-submit operation to default server or fail it.
     *
     * @param pubSubData
     *          Pub/Sub Operation
     */
    protected void retryOrFailOp(PubSubData pubSubData) {
        // if we were not able to connect to the host, it could be down
        ByteString hostString = ByteString.copyFromUtf8(HedwigSocketAddress.sockAddrStr(host));
        if (pubSubData.connectFailedServers != null &&
            pubSubData.connectFailedServers.contains(hostString)) {
            // We've already tried to connect to this host before so just
            // invoke the operationFailed callback.
            logger.error("Error connecting to host {} more than once so fail the request: {}",
                         va(host, pubSubData));
            pubSubData.getCallback().operationFailed(pubSubData.context,
                new CouldNotConnectException("Could not connect to host: " + host));
        } else {
            logger.error("Retry to connect to default hub server again for pubSubData: {}",
                         pubSubData);
            // Keep track of this current server that we failed to connect
            // to but retry the request on the default server host/VIP.
            if (pubSubData.connectFailedServers == null) {
                pubSubData.connectFailedServers = new LinkedList<ByteString>();
            }
            pubSubData.connectFailedServers.add(hostString);
            channelManager.submitOpToDefaultServer(pubSubData);
        }
    }

    private void onChannelConnected(ChannelFuture future) {
        Queue<PubSubData> oldPendingOps;
        synchronized (this) {
            // if the channel is closed by client, do nothing
            if (closed) {
                future.getChannel().close();
                return;
            }
            state = State.CONNECTED;
            channel = future.getChannel();
            host = NetUtils.getHostFromChannel(channel);
            oldPendingOps = pendingOps;
            pendingOps = new ArrayDeque<PubSubData>();
        }
        for (PubSubData op : oldPendingOps) {
            executeOpAfterConnected(op);
        }
    }

    private void onChannelConnectFailure() {
        Queue<PubSubData> oldPendingOps;
        synchronized (this) {
            state = State.DISCONNECTED;
            channel = null;
            oldPendingOps = pendingOps;
            pendingOps = new ArrayDeque<PubSubData>();
        }
        for (PubSubData op : oldPendingOps) {
            retryOrFailOp(op);
        }
    }

    private void connect() {
        synchronized (this) {
            if (State.CONNECTING == state ||
                State.CONNECTED == state) {
                return;
            }
            state = State.CONNECTING;
        }
        // Start the connection attempt to the input server host.
        ChannelFuture future = connect(host, pipelineFactory);
        future.addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                // If the channel has been closed, there is no need to proceed with any
                // callback logic here.
                if (closed) {
                    future.getChannel().close();
                    return;
                }

                if (!future.isSuccess()) {
                    logger.error("Error connecting to host {}.", host);
                    future.getChannel().close();

                    // if we were not able to connect to the host, it could be down.
                    onChannelConnectFailure();
                    return;
                }
                logger.debug("Connected to server {}.", host);
                // Now that we have connected successfully to the server, execute all queueing
                // requests.
                onChannelConnected(future);
            }

        });
    }

    /**
     * This is a helper method to do the connect attempt to the server given the
     * inputted host/port. This can be used to connect to the default server
     * host/port which is the VIP. That will pick a server in the cluster at
     * random to connect to for the initial PubSub attempt (with redirect logic
     * being done at the server side). Additionally, this could be called after
     * the client makes an initial PubSub attempt at a server, and is redirected
     * to the one that is responsible for the topic. Once the connect to the
     * server is done, we will perform the corresponding PubSub write on that
     * channel.
     *
     * @param serverHost
     *            Input server host to connect to of type InetSocketAddress
     * @param pipelineFactory
     *            PipelineFactory to create response handler to handle responses from
     *            underlying channel.
     */
    protected ChannelFuture connect(InetSocketAddress serverHost,
                                    ClientChannelPipelineFactory pipelineFactory) {
        logger.debug("Connecting to host {} ...", serverHost);
        // Set up the ClientBootStrap so we can create a new Channel connection
        // to the server.
        ClientBootstrap bootstrap = new ClientBootstrap(channelManager.getChannelFactory());
        bootstrap.setPipelineFactory(pipelineFactory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);

        // Start the connection attempt to the input server host.
        return bootstrap.connect(serverHost);
    }

    @Override
    public void close(boolean wait) {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        if (null == channel) {
            return;
        }
        try {
            getHChannelHandlerFromChannel(channel).closeExplicitly();
        } catch (NoResponseHandlerException nrhe) {
            logger.warn("No channel handler found for channel {} when closing it.",
                        channel);
        }
        if (wait) {
            channel.close().awaitUninterruptibly();
        } else {
            channel.close();
        }
        channel = null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[HChannel: host - ").append(host)
          .append(", channel - ").append(channel)
          .append(", pending reqs - ").append(pendingOps.size())
          .append(", closed - ").append(closed).append("]");
        return sb.toString();
    }

    @Override
    public void close() {
        close(false);
    }

    /**
     * Helper static method to get the ResponseHandler instance from a Channel
     * via the ChannelPipeline it is associated with. The assumption is that the
     * last ChannelHandler tied to the ChannelPipeline is the ResponseHandler.
     *
     * @param channel
     *            Channel we are retrieving the ResponseHandler instance for
     * @return ResponseHandler Instance tied to the Channel's Pipeline
     */
    public static HChannelHandler getHChannelHandlerFromChannel(Channel channel)
    throws NoResponseHandlerException {
        if (null == channel) {
            throw new NoResponseHandlerException("Received a null value for the channel. Cannot retrieve the response handler");
        }

        HChannelHandler handler = (HChannelHandler) channel.getPipeline().getLast();
        if (null == handler) {
            throw new NoResponseHandlerException("Could not retrieve the response handler from the channel's pipeline.");
        }
        return handler;
    }

}
