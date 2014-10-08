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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.ssl.SslHandler;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.exceptions.NoResponseHandlerException;
import org.apache.hedwig.client.netty.NetUtils;
import org.apache.hedwig.client.handlers.AbstractResponseHandler;
import org.apache.hedwig.client.handlers.SubscribeResponseHandler;
import org.apache.hedwig.exceptions.PubSubException.UncertainStateException;
import org.apache.hedwig.exceptions.PubSubException.UnexpectedConditionException;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEventResponse;
import static org.apache.hedwig.util.VarArgs.va;

public class HChannelHandler extends SimpleChannelHandler {

    private static Logger logger = LoggerFactory.getLogger(HChannelHandler.class);

    // Concurrent Map to store for each async PubSub request, the txn ID
    // and the corresponding PubSub call's data which stores the VoidCallback to
    // invoke when we receive a PubSub ack response from the server.
    // This is specific to this instance of the HChannelHandler which is
    // tied to a specific netty Channel Pipeline.
    private final ConcurrentMap<Long, PubSubData> txn2PubSubData =
        new ConcurrentHashMap<Long, PubSubData>();

    // Boolean indicating if we closed the channel this HChannelHandler is
    // attached to explicitly or not. If so, we do not need to do the
    // channel disconnected logic here.
    private volatile boolean channelClosedExplicitly = false;

    private final AbstractHChannelManager channelManager;
    private final ClientConfiguration cfg;

    private final Map<OperationType, AbstractResponseHandler> handlers;
    private final SubscribeResponseHandler subHandler;

    public HChannelHandler(ClientConfiguration cfg,
                           AbstractHChannelManager channelManager,
                           Map<OperationType, AbstractResponseHandler> handlers) {
        this.cfg = cfg;
        this.channelManager = channelManager;
        this.handlers = handlers;
        subHandler = (SubscribeResponseHandler) handlers.get(OperationType.SUBSCRIBE);
    }

    public SubscribeResponseHandler getSubscribeResponseHandler() {
        return subHandler;
    }

    public void removeTxn(long txnId) {
        txn2PubSubData.remove(txnId);
    }

    public void addTxn(long txnId, PubSubData pubSubData) {
        txn2PubSubData.put(txnId, pubSubData);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        // If the Message is not a PubSubResponse, just send it upstream and let
        // something else handle it.
        if (!(e.getMessage() instanceof PubSubResponse)) {
            ctx.sendUpstream(e);
            return;
        }
        // Retrieve the PubSubResponse from the Message that was sent by the
        // server.
        PubSubResponse response = (PubSubResponse) e.getMessage();
        logger.debug("Response received from host: {}, response: {}.",
                     va(NetUtils.getHostFromChannel(ctx.getChannel()), response));

        // Determine if this PubSubResponse is an ack response for a PubSub
        // Request or if it is a message being pushed to the client subscriber.
        if (response.hasMessage()) {
            // Subscribed messages being pushed to the client so handle/consume
            // it and return.
            if (null == subHandler) {
                logger.error("Received message from a non-subscription channel : {}",
                             response);
            } else {
                subHandler.handleSubscribeMessage(response);
            }
            return;
        }

        // Process Subscription Events
        if (response.hasResponseBody()) {
            ResponseBody resp = response.getResponseBody();
            // A special subscription event indicates the state of a subscriber
            if (resp.hasSubscriptionEvent()) {
                if (null == subHandler) {
                    logger.error("Received subscription event from a non-subscription channel : {}",
                                 response); 
                } else {
                    SubscriptionEventResponse eventResp = resp.getSubscriptionEvent();
                    logger.debug("Received subscription event {} for (topic:{}, subscriber:{}).",
                                 va(eventResp.getEvent(), response.getTopic(),
                                    response.getSubscriberId()));
                    subHandler.handleSubscriptionEvent(response.getTopic(),
                                                       response.getSubscriberId(),
                                                       eventResp.getEvent());
                }
                return;
            }
        }

        // Response is an ack to a prior PubSubRequest so first retrieve the
        // PubSub data for this txn.
        PubSubData pubSubData = txn2PubSubData.remove(response.getTxnId());

        // Validate that the PubSub data for this txn is stored. If not, just
        // log an error message and return since we don't know how to handle
        // this.
        if (pubSubData == null) {
            logger.error("PubSub Data was not found for PubSubResponse: {}", response);
            return;
        }

        // Store the topic2Host mapping if this wasn't a server redirect. We'll
        // assume that if the server was able to have an open Channel connection
        // to the client, and responded with an ack message other than the
        // NOT_RESPONSIBLE_FOR_TOPIC one, it is the correct topic master.
        if (!response.getStatusCode().equals(StatusCode.NOT_RESPONSIBLE_FOR_TOPIC)) {
            // Retrieve the server host that we've connected to and store the
            // mapping from the topic to this host. For all other non-redirected
            // server statuses, we consider that as a successful connection to the
            // correct topic master.
            InetSocketAddress host = NetUtils.getHostFromChannel(ctx.getChannel());
            channelManager.storeTopic2HostMapping(pubSubData.topic, host);
        }

        // Depending on the operation type, call the appropriate handler.
        logger.debug("Handling a {} response: {}, pubSubData: {}, host: {}.",
                     va(pubSubData.operationType, response, pubSubData, ctx.getChannel()));
        AbstractResponseHandler respHandler = handlers.get(pubSubData.operationType);
        if (null == respHandler) {
            // The above are the only expected PubSubResponse messages received
            // from the server for the various client side requests made.
            logger.error("Response received from server is for an unhandled operation {}, txnId: {}.",
                         va(pubSubData.operationType, response.getTxnId()));
            pubSubData.getCallback().operationFailed(pubSubData.context,
                new UnexpectedConditionException("Can't find response handler for operation "
                                                 + pubSubData.operationType));
            return;
        }
        respHandler.handleResponse(response, pubSubData, ctx.getChannel());
    }

    public void checkTimeoutRequests() {
        long curTime = System.currentTimeMillis();
        long timeoutInterval = cfg.getServerAckResponseTimeout();
        for (PubSubData pubSubData : txn2PubSubData.values()) {
            checkTimeoutRequest(pubSubData, curTime, timeoutInterval);
        }
    }

    private void checkTimeoutRequest(PubSubData pubSubData,
                                     long curTime, long timeoutInterval) {
        if (curTime > pubSubData.requestWriteTime + timeoutInterval) {
            // Current PubSubRequest has timed out so remove it from the
            // ResponseHandler's map and invoke the VoidCallback's
            // operationFailed method.
            logger.error("Current PubSubRequest has timed out for pubSubData: " + pubSubData);
            txn2PubSubData.remove(pubSubData.txnId);
            pubSubData.getCallback().operationFailed(pubSubData.context,
                new UncertainStateException("Server ack response never received so PubSubRequest has timed out!"));
        }
    }

    // Logic to deal with what happens when a Channel to a server host is
    // disconnected.
    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // If this channel was closed explicitly by the client code,
        // we do not need to do any of this logic. This could happen
        // for redundant Publish channels created or redirected subscribe
        // channels that are not used anymore or when we shutdown the
        // client and manually close all of the open channels.
        // Also don't do any of the disconnect logic if the client has stopped.
        if (channelClosedExplicitly || channelManager.isClosed()) {
            return;
        }

        // Make sure the host retrieved is not null as there could be some weird
        // channel disconnect events happening during a client shutdown.
        // If it is, just return as there shouldn't be anything we need to do.
        InetSocketAddress host = NetUtils.getHostFromChannel(ctx.getChannel());
        if (host == null) {
            return;
        }

        logger.info("Channel {} was disconnected to host {}.",
                    va(ctx.getChannel(), host));

        // If this Channel was used for Publish and Unsubscribe flows, just
        // remove it from the HewdigPublisher's host2Channel map. We will
        // re-establish a Channel connection to that server when the next
        // publish/unsubscribe request to a topic that the server owns occurs.

        // Now determine what type of operation this channel was used for.
        if (null == subHandler) {
            channelManager.onNonSubscriptionChannelDisconnected(host, ctx.getChannel());
        } else {
            channelManager.onSubscriptionChannelDisconnected(host, ctx.getChannel());
        }

        // Finally, all of the PubSubRequests that are still waiting for an ack
        // response from the server need to be removed and timed out. Invoke the
        // operationFailed callbacks on all of them. Use the
        // UncertainStateException since the server did receive the request but
        // we're not sure of the state of the request since the ack response was
        // never received.
        for (PubSubData pubSubData : txn2PubSubData.values()) {
            logger.debug("Channel disconnected so invoking the operationFailed callback for pubSubData: {}",
                         pubSubData);
            pubSubData.getCallback().operationFailed(pubSubData.context, new UncertainStateException(
                                                     "Server ack response never received before server connection disconnected!"));
        }
        txn2PubSubData.clear();
    }

    // Logic to deal with what happens when a Channel to a server host is
    // connected. This is needed if the client is using an SSL port to
    // communicate with the server. If so, we need to do the SSL handshake here
    // when the channel is first connected.
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // No need to initiate the SSL handshake if we are closing this channel
        // explicitly or the client has been stopped.
        if (cfg.isSSLEnabled() && !channelClosedExplicitly && !channelManager.isClosed()) {
            logger.debug("Initiating the SSL handshake");
            ctx.getPipeline().get(SslHandler.class).handshake();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.error("Exception caught on client channel", e.getCause());
        e.getChannel().close();
    }

    public void closeExplicitly() {
        // TODO: BOOKKEEPER-350 : Handle consume buffering, etc here - in a different patch
        channelClosedExplicitly = true;
    }
}
