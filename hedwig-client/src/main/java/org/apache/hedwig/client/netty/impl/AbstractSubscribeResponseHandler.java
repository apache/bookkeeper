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
import org.apache.hedwig.util.Either;
import org.apache.hedwig.util.SubscriptionListener;
import static org.apache.hedwig.util.VarArgs.va;

public abstract class AbstractSubscribeResponseHandler extends SubscribeResponseHandler {

    private static Logger logger =
        LoggerFactory.getLogger(AbstractSubscribeResponseHandler.class);

    protected final ReentrantReadWriteLock disconnectLock =
        new ReentrantReadWriteLock();

    protected final ConcurrentMap<TopicSubscriber, ActiveSubscriber> subscriptions
        = new ConcurrentHashMap<TopicSubscriber, ActiveSubscriber>();
    protected final AbstractHChannelManager aChannelManager;

    protected AbstractSubscribeResponseHandler(ClientConfiguration cfg,
                                               HChannelManager channelManager) {
        super(cfg, channelManager);
        this.aChannelManager = (AbstractHChannelManager) channelManager;
    }

    protected HChannelManager getHChannelManager() {
        return this.channelManager;
    }

    protected ClientConfiguration getConfiguration() {
        return cfg;
    }

    protected ActiveSubscriber getActiveSubscriber(TopicSubscriber ts) {
        return subscriptions.get(ts);
    }

    protected ActiveSubscriber createActiveSubscriber(
        ClientConfiguration cfg, AbstractHChannelManager channelManager,
        TopicSubscriber ts, PubSubData op, SubscriptionPreferences preferences,
        Channel channel, HChannel hChannel) {
        return new ActiveSubscriber(cfg, channelManager, ts, op, preferences, channel, hChannel);
    }

    @Override
    public void handleResponse(PubSubResponse response, PubSubData pubSubData,
                               Channel channel) throws Exception {
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

            Either<StatusCode, HChannel> result;
            StatusCode statusCode;
            ActiveSubscriber ss = null;
            // Store the Subscribe state
            disconnectLock.readLock().lock();
            try {
                result = handleSuccessResponse(ts, pubSubData, channel);
                statusCode = result.left();
                if (StatusCode.SUCCESS == statusCode) {
                    ss = createActiveSubscriber(
                        cfg, aChannelManager, ts, pubSubData, preferences, channel, result.right());
                    statusCode = addSubscription(ts, ss);
                }
            } finally {
                disconnectLock.readLock().unlock();
            }
            if (StatusCode.SUCCESS == statusCode) {
                postHandleSuccessResponse(ts, ss);
                // Response was success so invoke the callback's operationFinished
                // method.
                pubSubData.getCallback().operationFinished(pubSubData.context, null);
            } else {
                PubSubException exception = PubSubException.create(statusCode,
                    "Client is already subscribed for " + ts);
                pubSubData.getCallback().operationFailed(pubSubData.context, exception);
            }
            break;
        case CLIENT_ALREADY_SUBSCRIBED:
            // For Subscribe requests, the server says that the client is
            // already subscribed to it.
            pubSubData.getCallback().operationFailed(pubSubData.context,
                    new ClientAlreadySubscribedException("Client is already subscribed for topic: "
                                                         + pubSubData.topic.toStringUtf8() + ", subscriberId: "
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
            pubSubData.getCallback().operationFailed(pubSubData.context,
                    new ServiceDownException("Server responded with a status code of: "
                            + response.getStatusCode(),
                            PubSubException.create(response.getStatusCode(),
                                                   "Original Exception")));
            break;
        }
    }

    /**
     * Handle success response for a specific TopicSubscriber <code>ts</code>. The method
     * is triggered after subscribed successfully.
     *
     * @param ts
     *          Topic Subscriber.
     * @param pubSubData
     *          Pub/Sub Request data for this subscribe request.
     * @param channel
     *          Subscription Channel.
     * @return status code to indicate what happened
     */
    protected abstract Either<StatusCode, HChannel> handleSuccessResponse(
        TopicSubscriber ts, PubSubData pubSubData, Channel channel);

    protected void postHandleSuccessResponse(TopicSubscriber ts, ActiveSubscriber ss) {
        // do nothing now
    }

    private StatusCode addSubscription(TopicSubscriber ts, ActiveSubscriber ss) {
        ActiveSubscriber oldSS = subscriptions.putIfAbsent(ts, ss);
        if (null != oldSS) {
            return StatusCode.CLIENT_ALREADY_SUBSCRIBED;
        } else {
            return StatusCode.SUCCESS;
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
        ss.messageConsumed(message);
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
        processSubscriptionEvent(ss, event);
    }

    protected void processSubscriptionEvent(ActiveSubscriber as, SubscriptionEvent event) {
        switch (event) {
        // for all cases we need to resubscribe for the subscription
        case TOPIC_MOVED:
        case SUBSCRIPTION_FORCED_CLOSED:
            resubscribeIfNecessary(as, event);
            break;
        default:
            logger.error("Receive unknown subscription event {} for {}.",
                         va(event, as.getTopicSubscriber()));
        }
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
        logger.info("Subscription channel for ({}) is disconnected.", ss);
        resubscribeIfNecessary(ss, SubscriptionEvent.TOPIC_MOVED);
    }

    protected boolean removeSubscription(TopicSubscriber ts, ActiveSubscriber ss) {
        return subscriptions.remove(ts, ss);
    }

    protected void resubscribeIfNecessary(ActiveSubscriber ss, SubscriptionEvent event) {
        // if subscriber has been changed, we don't need to resubscribe
        if (!removeSubscription(ss.getTopicSubscriber(), ss)) {
            return;
        }
        ss.resubscribeIfNecessary(event);
    }

}
