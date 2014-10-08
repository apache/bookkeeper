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
package org.apache.hedwig.client.netty.impl.simple;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import com.google.protobuf.ByteString;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.handlers.SubscribeResponseHandler;
import org.apache.hedwig.client.netty.HChannel;
import org.apache.hedwig.client.netty.HChannelManager;
import org.apache.hedwig.client.netty.impl.AbstractHChannelManager;
import org.apache.hedwig.client.netty.impl.AbstractSubscribeResponseHandler;
import org.apache.hedwig.client.netty.impl.ActiveSubscriber;
import org.apache.hedwig.client.netty.impl.HChannelImpl;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protoextensions.MessageIdUtils;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.Either;

public class SimpleSubscribeResponseHandler extends AbstractSubscribeResponseHandler {

    private static Logger logger = LoggerFactory.getLogger(SimpleSubscribeResponseHandler.class);

    /**
     * Simple Active Subscriber enabling client-side throttling.
     */
    static class SimpleActiveSubscriber extends ActiveSubscriber {

        // Set to store all of the outstanding subscribed messages that are pending
        // to be consumed by the client app's MessageHandler. If this ever grows too
        // big (e.g. problem at the client end for message consumption), we can
        // throttle things by temporarily setting the Subscribe Netty Channel
        // to not be readable. When the Set has shrunk sufficiently, we can turn the
        // channel back on to read new messages.
        private final Set<Message> outstandingMsgSet;

        public SimpleActiveSubscriber(ClientConfiguration cfg,
                                      AbstractHChannelManager channelManager,
                                      TopicSubscriber ts, PubSubData op,
                                      SubscriptionPreferences preferences,
                                      Channel channel,
                                      HChannel hChannel) {
            super(cfg, channelManager, ts, op, preferences, channel, hChannel);
            outstandingMsgSet = Collections.newSetFromMap(
                    new ConcurrentHashMap<Message, Boolean>(
                            cfg.getMaximumOutstandingMessages(), 1.0f));
        }

        @Override
        protected void unsafeDeliverMessage(Message message) {
            // Add this "pending to be consumed" message to the outstandingMsgSet.
            outstandingMsgSet.add(message);
            // Check if we've exceeded the max size for the outstanding message set.
            if (outstandingMsgSet.size() >= cfg.getMaximumOutstandingMessages() &&
                channel.isReadable()) {
                // Too many outstanding messages so throttle it by setting the Netty
                // Channel to not be readable.
                if (logger.isDebugEnabled()) {
                    logger.debug("Too many outstanding messages ({}) so throttling the subscribe netty Channel",
                                 outstandingMsgSet.size());
                }
                channel.setReadable(false);
            }
            super.unsafeDeliverMessage(message);
        }

        @Override
        public synchronized void messageConsumed(Message message) {
            super.messageConsumed(message);
            // Remove this consumed message from the outstanding Message Set.
            outstandingMsgSet.remove(message);
            // Check if we throttled message consumption previously when the
            // outstanding message limit was reached. For now, only turn the
            // delivery back on if there are no more outstanding messages to
            // consume. We could make this a configurable parameter if needed.
            if (!channel.isReadable() && outstandingMsgSet.size() == 0) {
                if (logger.isDebugEnabled())
                    logger.debug("Message consumption has caught up so okay to turn off"
                                 + " throttling of messages on the subscribe channel for {}",
                                 topicSubscriber);
                channel.setReadable(true);
            }
        }

        @Override
        public synchronized void startDelivery(MessageHandler messageHandler)
        throws AlreadyStartDeliveryException, ClientNotSubscribedException {
            super.startDelivery(messageHandler);
            // Now make the TopicSubscriber Channel readable (it is set to not be
            // readable when the initial subscription is done). Note that this is an
            // asynchronous call. If this fails (not likely), the futureListener
            // will just log an error message for now.
            ChannelFuture future = channel.setReadable(true);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        logger.error("Unable to make subscriber Channel readable in startDelivery call for {}",
                                     topicSubscriber);
                    }
                }
            });
        }

        @Override
        public synchronized void stopDelivery() {
            super.stopDelivery();
            // Now make the TopicSubscriber channel not-readable. This will buffer
            // up messages if any are sent from the server. Note that this is an
            // asynchronous call. If this fails (not likely), the futureListener
            // will just log an error message for now.
            ChannelFuture future = channel.setReadable(false);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        logger.error("Unable to make subscriber Channel not readable in stopDelivery call for {}",
                                     topicSubscriber);
                    }
                }
            });
        }

    }

    // Track which subscriber is alive in this response handler
    // Which is used for backward compat, since old version hub
    // server doesn't carry (topic, subscriberid) in each message.
    private volatile TopicSubscriber origTopicSubscriber;
    private volatile ActiveSubscriber origActiveSubscriber;

    private SimpleHChannelManager sChannelManager;

    protected SimpleSubscribeResponseHandler(ClientConfiguration cfg,
                                             HChannelManager channelManager) {
        super(cfg, channelManager);
        sChannelManager = (SimpleHChannelManager) channelManager;
    }

    @Override
    protected ActiveSubscriber createActiveSubscriber(
        ClientConfiguration cfg, AbstractHChannelManager channelManager,
        TopicSubscriber ts, PubSubData op, SubscriptionPreferences preferences,
        Channel channel, HChannel hChannel) {
        return new SimpleActiveSubscriber(cfg, channelManager, ts, op, preferences, channel, hChannel);
    }

    @Override
    protected synchronized ActiveSubscriber getActiveSubscriber(TopicSubscriber ts) {
        if (null == origTopicSubscriber || !origTopicSubscriber.equals(ts)) {
            return null;
        }
        return origActiveSubscriber;
    }

    private synchronized ActiveSubscriber getActiveSubscriber() {
        return origActiveSubscriber;
    }

    @Override
    public synchronized boolean hasSubscription(TopicSubscriber ts) {
        if (null == origTopicSubscriber) {
            return false;
        }
        return origTopicSubscriber.equals(ts);
    }

    @Override
    protected synchronized boolean removeSubscription(TopicSubscriber ts, ActiveSubscriber ss) {
        if (null != origTopicSubscriber && !origTopicSubscriber.equals(ts)) {
            return false;
        }
        origTopicSubscriber = null;
        origActiveSubscriber = null;
        return super.removeSubscription(ts, ss);
    }

    @Override
    public void handleResponse(PubSubResponse response, PubSubData pubSubData,
                               Channel channel) throws Exception {
        // If this was not a successful response to the Subscribe request, we
        // won't be using the Netty Channel created so just close it.
        if (!response.getStatusCode().equals(StatusCode.SUCCESS)) {
            HChannelImpl.getHChannelHandlerFromChannel(channel).closeExplicitly();
            channel.close();
        }
        super.handleResponse(response, pubSubData, channel);
    }

    @Override
    public void handleSubscribeMessage(PubSubResponse response) {
        Message message = response.getMessage();
        ActiveSubscriber ss = getActiveSubscriber();
        if (null == ss) {
            logger.error("No Subscriber is alive receiving its message {}.",
                         MessageIdUtils.msgIdToReadableString(message.getMsgId()));
            return;
        }
        ss.handleMessage(message);
    }

    @Override
    protected Either<StatusCode, HChannel> handleSuccessResponse(
        TopicSubscriber ts, PubSubData pubSubData, Channel channel) {
        // Store the mapping for the TopicSubscriber to the Channel.
        // This is so we can control the starting and stopping of
        // message deliveries from the server on that Channel. Store
        // this only on a successful ack response from the server.
        Either<Boolean, HChannel> result =
            sChannelManager.storeSubscriptionChannel(ts, pubSubData, channel);
        if (result.left()) {
            return Either.of(StatusCode.SUCCESS, result.right());
        } else {
            StatusCode code;
            if (pubSubData.isResubscribeRequest()) {
                code = StatusCode.RESUBSCRIBE_EXCEPTION;
            } else {
                code = StatusCode.CLIENT_ALREADY_SUBSCRIBED;
            }
            return Either.of(code, null);
        }
    }

    @Override
    protected synchronized void postHandleSuccessResponse(
        TopicSubscriber ts, ActiveSubscriber as) {
        origTopicSubscriber = ts;
        origActiveSubscriber = as;
    }

    @Override
    public void asyncCloseSubscription(final TopicSubscriber topicSubscriber,
                                       final Callback<ResponseBody> callback,
                                       final Object context) {
        // nothing to do just clear status
        // channel manager takes the responsibility to close the channel
        callback.operationFinished(context, (ResponseBody)null);
    }

}
