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
package org.apache.hedwig.client.netty.impl.multiplex;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.MessageConsumeData;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.handlers.SubscribeResponseHandler;
import org.apache.hedwig.client.netty.HChannelManager;
import org.apache.hedwig.client.netty.HChannel;
import org.apache.hedwig.client.netty.NetUtils;
import org.apache.hedwig.client.netty.FilterableMessageHandler;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.exceptions.PubSubException.UnexpectedConditionException;
import org.apache.hedwig.filter.ClientMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.protoextensions.MessageIdUtils;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.SubscriptionListener;
import static org.apache.hedwig.util.VarArgs.va;

public class MultiplexSubscribeResponseHandler extends SubscribeResponseHandler {

    private static Logger logger =
        LoggerFactory.getLogger(MultiplexSubscribeResponseHandler.class);

    class ActiveSubscriber implements SubscriptionListener {
        private final TopicSubscriber topicSubscriber;
        private final PubSubData op;
        private final SubscriptionPreferences preferences;

        // the underlying netty channel to send request
        private final Channel channel;

        // Counter for the number of consumed messages so far to buffer up before we
        // send the Consume message back to the server along with the last/largest
        // message seq ID seen so far in that batch.
        private int numConsumedMessagesInBuffer = 0;
        private MessageSeqId lastMessageSeqId;

        // Message Handler
        private MessageHandler msgHandler;

        // Queue used for subscribes when the MessageHandler hasn't been registered
        // yet but we've already received subscription messages from the server.
        // This will be lazily created as needed.
        private Queue<Message> msgQueue = new LinkedList<Message>();

        ActiveSubscriber(TopicSubscriber ts, PubSubData op,
                        SubscriptionPreferences preferences,
                        Channel channel) {
            this.topicSubscriber = ts;
            this.op = op;
            this.preferences = preferences;
            this.channel = channel;
        }

        PubSubData getPubSubData() {
            return this.op;
        }

        TopicSubscriber getTopicSubscriber() {
            return this.topicSubscriber;
        }

        synchronized boolean updateLastMessageSeqId(MessageSeqId seqId) {
            if (null != lastMessageSeqId &&
                seqId.getLocalComponent() <= lastMessageSeqId.getLocalComponent()) {
                return false;
            }
            ++numConsumedMessagesInBuffer;
            lastMessageSeqId = seqId;
            if (numConsumedMessagesInBuffer >= cfg.getConsumedMessagesBufferSize()) {
                numConsumedMessagesInBuffer = 0;
                lastMessageSeqId = null;
                return true;
            }
            return false;
        }

        synchronized void startDelivery(MessageHandler messageHandler)
        throws AlreadyStartDeliveryException, ClientNotSubscribedException {
            if (null != this.msgHandler) {
                throw new AlreadyStartDeliveryException("A message handler " + msgHandler 
                    + " has been started for " + topicSubscriber);
            }
            if (null != messageHandler && messageHandler instanceof FilterableMessageHandler) {
                FilterableMessageHandler filterMsgHandler =
                    (FilterableMessageHandler) messageHandler;
                if (filterMsgHandler.hasMessageFilter()) {
                    if (null == preferences) {
                        // no preferences means talking to an old version hub server
                        logger.warn("Start delivering messages with filter but no subscription "
                                  + "preferences found. It might due to talking to an old version"
                                  + " hub server.");
                        // use the original message handler.
                        messageHandler = filterMsgHandler.getMessageHandler();
                    } else {
                        // pass subscription preferences to message filter
                        if (logger.isDebugEnabled()) {
                            logger.debug("Start delivering messages with filter on {}, preferences: {}",
                                         va(topicSubscriber,
                                            SubscriptionStateUtils.toString(preferences)));
                        }
                        ClientMessageFilter msgFilter = filterMsgHandler.getMessageFilter();
                        msgFilter.setSubscriptionPreferences(topicSubscriber.getTopic(),
                                                             topicSubscriber.getSubscriberId(),
                                                             preferences);
                    }
                }
            }

            this.msgHandler = messageHandler;
            // Once the MessageHandler is registered, see if we have any queued up
            // subscription messages sent to us already from the server. If so,
            // consume those first. Do this only if the MessageHandler registered is
            // not null (since that would be the HedwigSubscriber.stopDelivery
            // call).
            if (null == msgHandler) {
                return;
            }
            if (msgQueue.size() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Consuming {} queued up messages for {}",
                                 va(msgQueue.size(), topicSubscriber));
                }
                for (Message message : msgQueue) {
                    asyncMessageDeliver(message);
                }
                // Now we can remove the queued up messages since they are all
                // consumed.
                msgQueue.clear();
            }
        }

        synchronized void stopDelivery() {
            this.msgHandler = null;
        }

        synchronized void handleMessage(Message message) {
            if (null != msgHandler) {
                asyncMessageDeliver(message);
            } else {
                // MessageHandler has not yet been registered so queue up these
                // messages for the Topic Subscription. Make the initial lazy
                // creation of the message queue thread safe just so we don't
                // run into a race condition where two simultaneous threads process
                // a received message and both try to create a new instance of
                // the message queue. Performance overhead should be okay
                // because the delivery of the topic has not even started yet
                // so these messages are not consumed and just buffered up here.
                if (logger.isDebugEnabled()) {
                    logger.debug("Message {} has arrived but no MessageHandler provided for {}"
                                 + " yet so queueing up the message.",
                                 va(MessageIdUtils.msgIdToReadableString(message.getMsgId()),
                                    topicSubscriber));
                }
                msgQueue.add(message);
            }
        }

        synchronized void asyncMessageDeliver(Message message) {
            if (null == msgHandler) {
                logger.error("No message handler found to deliver message {} to {}.",
                             va(MessageIdUtils.msgIdToReadableString(message.getMsgId()),
                                topicSubscriber));
                return;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Call the client app's MessageHandler asynchronously to deliver the message {} to {}",
                             va(message, topicSubscriber));
            }
            MessageConsumeData messageConsumeData =
                new MessageConsumeData(topicSubscriber, message);
            msgHandler.deliver(topicSubscriber.getTopic(), topicSubscriber.getSubscriberId(),
                               message, sChannelManager.getConsumeCallback(),
                               messageConsumeData);
        }

        void consume(final MessageSeqId messageSeqId) {
            PubSubRequest.Builder pubsubRequestBuilder =
                NetUtils.buildConsumeRequest(sChannelManager.nextTxnId(),
                                             topicSubscriber, messageSeqId);  

            // For Consume requests, we will send them from the client in a fire and
            // forget manner. We are not expecting the server to send back an ack
            // response so no need to register this in the ResponseHandler. There
            // are no callbacks to invoke since this isn't a client initiated
            // action. Instead, just have a future listener that will log an error
            // message if there was a problem writing the consume request.
            if (logger.isDebugEnabled()) {
                logger.debug("Writing a Consume request to host: {} with messageSeqId: {} for {}",
                             va(NetUtils.getHostFromChannel(channel),
                                messageSeqId, topicSubscriber));
            }
            ChannelFuture future = channel.write(pubsubRequestBuilder.build());
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        logger.error("Error writing a Consume request to host: {} with messageSeqId: {} for {}",
                                     va(host, messageSeqId, topicSubscriber));
                    }
                }
            });
        }

        @Override
        public void processEvent(ByteString topic, ByteString subscriberId,
                                 SubscriptionEvent event) {
            switch (event) {
            // for all cases we need to resubscribe for the subscription
            case TOPIC_MOVED:
                sChannelManager.clearHostForTopic(topic, NetUtils.getHostFromChannel(channel));
                resubscribeIfNecessary(event);
                break;
            case SUBSCRIPTION_FORCED_CLOSED:
                resubscribeIfNecessary(event);
                break;
            default:
                logger.error("Receive unknown subscription event {} for {}.",
                             va(event, topicSubscriber));
            }
        }

        private void resubscribeIfNecessary(SubscriptionEvent event) {
            // if subscriber has been changed, we don't need to resubscribe
            if (!subscriptions.remove(topicSubscriber, this)) {
                return;
            }
            if (!op.options.getEnableResubscribe()) {
                sChannelManager.getSubscriptionEventEmitter().emitSubscriptionEvent(
                    topicSubscriber.getTopic(), topicSubscriber.getSubscriberId(), event);
                return;
            }
            // Since the connection to the server host that was responsible
            // for the topic died, we are not sure about the state of that
            // server. Resend the original subscribe request data to the default
            // server host/VIP. Also clear out all of the servers we've
            // contacted or attempted to from this request as we are starting a
            // "fresh" subscribe request.
            op.clearServersList();
            // Set a new type of VoidCallback for this async call. We need this
            // hook so after the resubscribe has completed, delivery for
            // that topic subscriber should also be restarted (if it was that
            // case before the channel disconnect).
            final long retryWaitTime = cfg.getSubscribeReconnectRetryWaitTime();
            ResubscribeCallback resubscribeCb =
                new ResubscribeCallback(topicSubscriber, op,
                                        sChannelManager, retryWaitTime);
            op.setCallback(resubscribeCb);
            op.context = null;
            if (logger.isDebugEnabled()) {
                logger.debug("Resubscribe {} with origSubData {}",
                             va(topicSubscriber, op));
            }
            // resubmit the request
            sChannelManager.submitOp(op);
        }
    }

    protected final ReentrantReadWriteLock disconnectLock =
        new ReentrantReadWriteLock();

    // the underlying subscription channel
    volatile HChannel hChannel;
    InetSocketAddress host; 
    protected final ConcurrentMap<TopicSubscriber, ActiveSubscriber> subscriptions
        = new ConcurrentHashMap<TopicSubscriber, ActiveSubscriber>();
    private final MultiplexHChannelManager sChannelManager;

    protected MultiplexSubscribeResponseHandler(ClientConfiguration cfg,
                                                HChannelManager channelManager) {
        super(cfg, channelManager);
        sChannelManager = (MultiplexHChannelManager) channelManager;
    }

    protected HChannelManager getHChannelManager() {
        return this.sChannelManager;
    }

    protected ClientConfiguration getConfiguration() {
        return cfg;
    }

    private ActiveSubscriber getActiveSubscriber(TopicSubscriber ts) {
        return subscriptions.get(ts);
    }

    @Override
    public void handleResponse(PubSubResponse response, PubSubData pubSubData,
                               Channel channel) throws Exception {
        if (null == hChannel) {
            host = NetUtils.getHostFromChannel(channel);
            hChannel = sChannelManager.getSubscriptionChannel(host);
            if (null == hChannel ||
                !channel.equals(hChannel.getChannel())) {
                PubSubException pse =
                    new UnexpectedConditionException("Failed to get subscription channel of " + host);
                pubSubData.getCallback().operationFailed(pubSubData.context, pse);
                return;
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Handling a Subscribe response: {}, pubSubData: {}, host: {}.",
                         va(response, pubSubData, NetUtils.getHostFromChannel(channel)));
        }
        switch (response.getStatusCode()) {
        case SUCCESS:
            TopicSubscriber ts = new TopicSubscriber(pubSubData.topic,
                                                     pubSubData.subscriberId);
            SubscriptionPreferences preferences = null;
            if (response.hasResponseBody()) {
                ResponseBody respBody = response.getResponseBody(); 
                if (respBody.hasSubscribeResponse()) {
                    SubscribeResponse resp = respBody.getSubscribeResponse();
                    if (resp.hasPreferences()) {
                        preferences = resp.getPreferences();
                        if (logger.isDebugEnabled()) {
                            logger.debug("Receive subscription preferences for {} : {}",
                                         va(ts,
                                            SubscriptionStateUtils.toString(preferences)));
                        }
                    }
                }
            }

            ActiveSubscriber ss = new ActiveSubscriber(ts, pubSubData, preferences,
                                                       channel);

            boolean success = false;
            // Store the Subscribe state
            disconnectLock.readLock().lock();
            try {
                ActiveSubscriber oldSS = subscriptions.putIfAbsent(ts, ss);
                if (null != oldSS) {
                    logger.warn("Subscribe {} has existed in channel {}.",
                                va(ts, channel));
                    success = false;
                } else {
                    logger.debug("Succeed to add subscription {} in channel {}.",
                                 va(ts, channel));
                    success = true;
                }
            } finally {
                disconnectLock.readLock().unlock();
            }
            if (success) {
                // Response was success so invoke the callback's operationFinished
                // method.
                pubSubData.getCallback().operationFinished(pubSubData.context, null);
            } else {
                ClientAlreadySubscribedException exception =
                    new ClientAlreadySubscribedException("Client is already subscribed for " + ts);
                pubSubData.getCallback().operationFailed(pubSubData.context, exception);
            }
            break;
        case CLIENT_ALREADY_SUBSCRIBED:
            // For Subscribe requests, the server says that the client is
            // already subscribed to it.
            pubSubData.getCallback().operationFailed(pubSubData.context, new ClientAlreadySubscribedException(
                                                     "Client is already subscribed for topic: " + pubSubData.topic.toStringUtf8() + ", subscriberId: "
                                                     + pubSubData.subscriberId.toStringUtf8()));
            break;
        case SERVICE_DOWN:
            // Response was service down failure so just invoke the callback's
            // operationFailed method.
            pubSubData.getCallback().operationFailed(pubSubData.context, new ServiceDownException(
                                                     "Server responded with a SERVICE_DOWN status"));
            break;
        case NOT_RESPONSIBLE_FOR_TOPIC:
            // Redirect response so we'll need to repost the original Subscribe
            // Request
            handleRedirectResponse(response, pubSubData, channel);
            break;
        default:
            // Consider all other status codes as errors, operation failed
            // cases.
            logger.error("Unexpected error response from server for PubSubResponse: " + response);
            pubSubData.getCallback().operationFailed(pubSubData.context, new ServiceDownException(
                                                     "Server responded with a status code of: " + response.getStatusCode()));
            break;
        }
    }

    @Override
    public void handleSubscribeMessage(PubSubResponse response) {
        Message message = response.getMessage();
        TopicSubscriber ts = new TopicSubscriber(response.getTopic(),
                                                 response.getSubscriberId());
        if (logger.isDebugEnabled()) {
            logger.debug("Handling a Subscribe message in response: {}, {}",
                         va(response, ts));
        }
        ActiveSubscriber ss = getActiveSubscriber(ts);
        if (null == ss) {
            logger.error("Subscriber {} is not found receiving its message {}.",
                         va(ts, MessageIdUtils.msgIdToReadableString(message.getMsgId())));
            return;
        }
        ss.handleMessage(message);
    }

    @Override
    protected void asyncMessageDeliver(TopicSubscriber topicSubscriber,
                                       Message message) {
        ActiveSubscriber ss = getActiveSubscriber(topicSubscriber);
        if (null == ss) {
            logger.error("Subscriber {} is not found delivering its message {}.",
                         va(topicSubscriber,
                            MessageIdUtils.msgIdToReadableString(message.getMsgId())));
            return;
        }
        ss.asyncMessageDeliver(message);
    }

    @Override
    protected void messageConsumed(TopicSubscriber topicSubscriber,
                                   Message message) {
        ActiveSubscriber ss = getActiveSubscriber(topicSubscriber);
        if (null == ss) {
            logger.warn("Subscriber {} is not found consumed its message {}.",
                        va(topicSubscriber,
                           MessageIdUtils.msgIdToReadableString(message.getMsgId())));
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Message has been successfully consumed by the client app : {}, {}",
                         va(message, topicSubscriber));
        }
        // For consume response to server, there is a config param on how many
        // messages to consume and buffer up before sending the consume request.
        // We just need to keep a count of the number of messages consumed
        // and the largest/latest msg ID seen so far in this batch. Messages
        // should be delivered in order and without gaps. Do this only if
        // auto-sending of consume messages is enabled.
        if (cfg.isAutoSendConsumeMessageEnabled()) {
            // Update these variables only if we are auto-sending consume
            // messages to the server. Otherwise the onus is on the client app
            // to call the Subscriber consume API to let the server know which
            // messages it has successfully consumed.
            if (ss.updateLastMessageSeqId(message.getMsgId())) {
                // Send the consume request and reset the consumed messages buffer
                // variables. We will use the same Channel created from the
                // subscribe request for the TopicSubscriber.
                if (logger.isDebugEnabled()) {
                    logger.debug("Consume message {} when reaching consumed message buffer limit.",
                                 message.getMsgId());
                }
                ss.consume(message.getMsgId());
            }
        }
    }

    @Override
    public void handleSubscriptionEvent(ByteString topic, ByteString subscriberId,
                                        SubscriptionEvent event) {
        TopicSubscriber ts = new TopicSubscriber(topic, subscriberId);
        ActiveSubscriber ss = getActiveSubscriber(ts);
        if (null == ss) {
            logger.warn("No subscription {} found receiving subscription event {}.",
                        va(ts, event));
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Received subscription event {} for ({}).",
                         va(event, ts));
        }
        ss.processEvent(topic, subscriberId, event);
    }

    @Override
    public void startDelivery(final TopicSubscriber topicSubscriber,
                              MessageHandler messageHandler)
    throws ClientNotSubscribedException, AlreadyStartDeliveryException {
        ActiveSubscriber ss = getActiveSubscriber(topicSubscriber);
        if (null == ss) {
            throw new ClientNotSubscribedException("Client is not yet subscribed to " + topicSubscriber);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Start delivering message for {} using message handler {}",
                         va(topicSubscriber, messageHandler));
        }
        ss.startDelivery(messageHandler); 
    }

    @Override
    public void stopDelivery(final TopicSubscriber topicSubscriber)
    throws ClientNotSubscribedException {
        ActiveSubscriber ss = getActiveSubscriber(topicSubscriber);
        if (null == ss) {
            throw new ClientNotSubscribedException("Client is not yet subscribed to " + topicSubscriber);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Stop delivering messages for {}", topicSubscriber);
        }
        ss.stopDelivery();
    }

    @Override
    public boolean hasSubscription(TopicSubscriber topicSubscriber) {
        return subscriptions.containsKey(topicSubscriber);
    }

    @Override
    public void asyncCloseSubscription(final TopicSubscriber topicSubscriber,
                                       final Callback<ResponseBody> callback,
                                       final Object context) {
        final ActiveSubscriber ss = getActiveSubscriber(topicSubscriber);
        if (null == ss || null == hChannel) {
            logger.debug("No subscription {} found when closing its subscription from {}.",
                         va(topicSubscriber, hChannel));
            callback.operationFinished(context, (ResponseBody)null);
            return;
        }
        Callback<ResponseBody> closeCb = new Callback<ResponseBody>() {
            @Override
            public void operationFinished(Object ctx, ResponseBody respBody) {
                disconnectLock.readLock().lock();
                try {
                    subscriptions.remove(topicSubscriber, ss);
                } finally {
                    disconnectLock.readLock().unlock();
                }
                callback.operationFinished(context, null);
            }

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                callback.operationFailed(context, exception);
            }
        };
        PubSubData closeOp = new PubSubData(topicSubscriber.getTopic(), null,
                                            topicSubscriber.getSubscriberId(),
                                            OperationType.CLOSESUBSCRIPTION,
                                            null, closeCb, context);
        hChannel.submitOp(closeOp);
    }

    @Override
    public void consume(final TopicSubscriber topicSubscriber,
                        final MessageSeqId messageSeqId) {
        ActiveSubscriber ss = getActiveSubscriber(topicSubscriber);
        if (null == ss) {
            logger.warn("Subscriber {} is not found consuming message {}.",
                        va(topicSubscriber,
                           MessageIdUtils.msgIdToReadableString(messageSeqId)));
            return;
        }
        ss.consume(messageSeqId); 
    }

    @Override
    public void onChannelDisconnected(InetSocketAddress host, Channel channel) {
        disconnectLock.writeLock().lock();
        try {
            onDisconnect(host);
        } finally {
            disconnectLock.writeLock().unlock();
        }
    }

    private void onDisconnect(InetSocketAddress host) {
        for (ActiveSubscriber ss : subscriptions.values()) {
            onDisconnect(ss, host);
        }
    }

    private void onDisconnect(ActiveSubscriber ss, InetSocketAddress host) {
        TopicSubscriber ts = ss.getTopicSubscriber();
        logger.info("Subscription channel for ({}) is disconnected.", ts);
        ss.processEvent(ts.getTopic(), ts.getSubscriberId(),
                        SubscriptionEvent.TOPIC_MOVED);
    }

}
