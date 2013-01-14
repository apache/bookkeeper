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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.NoResponseHandlerException;
import org.apache.hedwig.client.handlers.SubscribeResponseHandler;
import org.apache.hedwig.client.netty.CleanupChannelMap;
import org.apache.hedwig.client.netty.HChannel;
import org.apache.hedwig.client.netty.NetUtils;
import org.apache.hedwig.client.netty.impl.AbstractHChannelManager;
import org.apache.hedwig.client.netty.impl.ClientChannelPipelineFactory;
import org.apache.hedwig.client.netty.impl.HChannelHandler;
import org.apache.hedwig.client.netty.impl.HChannelImpl;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.exceptions.PubSubException.TopicBusyException;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.Either;
import static org.apache.hedwig.util.VarArgs.va;

/**
 * Simple HChannel Manager which establish a connection for each subscription.
 */
public class SimpleHChannelManager extends AbstractHChannelManager {

    private static Logger logger = LoggerFactory.getLogger(SimpleHChannelManager.class);

    // Concurrent Map to store the cached Channel connections on the client side
    // to a server host for a given Topic + SubscriberId combination. For each
    // TopicSubscriber, we want a unique Channel connection to the server for
    // it. We can also get the ResponseHandler tied to the Channel via the
    // Channel Pipeline.
    protected final CleanupChannelMap<TopicSubscriber> topicSubscriber2Channel;

    // Concurrent Map to store Message handler for each topic + sub id combination.
    // Store it here instead of in SubscriberResponseHandler as we don't want to lose the handler
    // user set when connection is recovered
    protected final ConcurrentMap<TopicSubscriber, MessageHandler> topicSubscriber2MessageHandler
        = new ConcurrentHashMap<TopicSubscriber, MessageHandler>();

    // PipelineFactory to create subscription netty channels to the appropriate server
    private final ClientChannelPipelineFactory subscriptionChannelPipelineFactory;

    public SimpleHChannelManager(ClientConfiguration cfg,
                                 ChannelFactory socketFactory) {
        super(cfg, socketFactory);
        topicSubscriber2Channel = new CleanupChannelMap<TopicSubscriber>();
        this.subscriptionChannelPipelineFactory =
            new SimpleSubscriptionChannelPipelineFactory(cfg, this);
    }

    @Override
    public void submitOp(final PubSubData pubSubData) {
        /**
         * In the simple hchannel implementation that if a client closes a subscription
         * and tries to attach to it immediately, it could get a TOPIC_BUSY response. This
         * is because, a subscription is closed simply by closing the channel, and the hub
         * side may not have been notified of the channel disconnection event by the time
         * the new subscription request comes in. To solve this, retry up to 5 times.
         * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-513}
         */
        if (OperationType.SUBSCRIBE.equals(pubSubData.operationType)) {
            final Callback<ResponseBody> origCb = pubSubData.getCallback();
            final AtomicInteger retries = new AtomicInteger(5);
            final Callback<ResponseBody> wrapperCb
                = new Callback<ResponseBody>() {
                @Override
                public void operationFinished(Object ctx,
                                              ResponseBody resultOfOperation) {
                    origCb.operationFinished(ctx, resultOfOperation);
                }

                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    if (exception instanceof ServiceDownException
                        && exception.getCause() instanceof TopicBusyException
                        && retries.decrementAndGet() > 0) {
                        logger.warn("TOPIC_DOWN from server using simple channel scheme."
                                    + "This could be due to the channel disconnection from a close"
                                    + " not having been triggered on the server side. Retrying");
                        SimpleHChannelManager.super.submitOp(pubSubData);
                        return;
                    }
                    origCb.operationFailed(ctx, exception);
                }
            };
            pubSubData.setCallback(wrapperCb);
        }
        super.submitOp(pubSubData);
    }

    @Override
    protected ClientChannelPipelineFactory getSubscriptionChannelPipelineFactory() {
        return subscriptionChannelPipelineFactory;
    }

    @Override
    protected HChannel createAndStoreSubscriptionChannel(Channel channel) {
        // for simple channel, we don't store subscription channel now
        // we store it until we received success response
        InetSocketAddress host = NetUtils.getHostFromChannel(channel);
        return new HChannelImpl(host, channel, this,
                                getSubscriptionChannelPipelineFactory());
    }

    @Override
    protected HChannel createAndStoreSubscriptionChannel(InetSocketAddress host) {
        // for simple channel, we don't store subscription channel now
        // we store it until we received success response
        return new HChannelImpl(host, this,
                                getSubscriptionChannelPipelineFactory());
    }

    protected Either<Boolean, HChannel> storeSubscriptionChannel(
        TopicSubscriber topicSubscriber, PubSubData txn, Channel channel) {
        InetSocketAddress host = NetUtils.getHostFromChannel(channel);
        HChannel newHChannel = new HChannelImpl(host, channel, this,
                                                getSubscriptionChannelPipelineFactory());
        boolean replaced = topicSubscriber2Channel.replaceChannel(
            topicSubscriber, txn.getOriginalChannelForResubscribe(), newHChannel);
        if (replaced) {
            return Either.of(replaced, newHChannel);
        } else {
            return Either.of(replaced, null);
        }
    }

    @Override
    protected HChannel getSubscriptionChannel(InetSocketAddress host) {
        return null;
    }

    @Override
    protected HChannel getSubscriptionChannelByTopicSubscriber(TopicSubscriber subscriber) {
        HChannel channel = topicSubscriber2Channel.getChannel(subscriber);
        if (null != channel) {
            // there is no channel established for this subscription
            return channel;
        } else {
            InetSocketAddress host = topic2Host.get(subscriber.getTopic());
            if (null == host) {
                return null;
            } else {
                channel = getSubscriptionChannel(host);
                if (null == channel) {
                    channel = createAndStoreSubscriptionChannel(host);
                }
                return channel;
            }
        }
    }

    @Override
    protected void onSubscriptionChannelDisconnected(InetSocketAddress host,
                                                     Channel channel) {
        logger.info("Subscription Channel {} disconnected from {}.",
                    va(channel, host));
        try {
            // get hchannel handler
            HChannelHandler channelHandler =
                HChannelImpl.getHChannelHandlerFromChannel(channel);
            channelHandler.getSubscribeResponseHandler()
                          .onChannelDisconnected(host, channel);
        } catch (NoResponseHandlerException nrhe) {
            logger.warn("No Channel Handler found for channel {} when it disconnected.",
                        channel);
        }
    }

    @Override
    public SubscribeResponseHandler getSubscribeResponseHandler(TopicSubscriber topicSubscriber) {
        HChannel hChannel = topicSubscriber2Channel.getChannel(topicSubscriber);
        if (null == hChannel) {
            return null;
        }
        Channel channel = hChannel.getChannel();
        if (null == channel) {
            return null;
        }
        try {
            HChannelHandler channelHandler =
                HChannelImpl.getHChannelHandlerFromChannel(channel);
            return channelHandler.getSubscribeResponseHandler();
        } catch (NoResponseHandlerException nrhe) {
            logger.warn("No Channel Handler found for channel {}, topic subscriber {}.",
                        channel, topicSubscriber);
            return null;
        }

    }

    @Override
    public void startDelivery(TopicSubscriber topicSubscriber,
                              MessageHandler messageHandler)
        throws ClientNotSubscribedException, AlreadyStartDeliveryException {
        startDelivery(topicSubscriber, messageHandler, false);
    }

    @Override
    protected void restartDelivery(TopicSubscriber topicSubscriber)
        throws ClientNotSubscribedException, AlreadyStartDeliveryException {
        startDelivery(topicSubscriber, null, true);
    }

    private void startDelivery(TopicSubscriber topicSubscriber,
                               MessageHandler messageHandler, boolean restart)
        throws ClientNotSubscribedException, AlreadyStartDeliveryException {
        // Make sure we know about this topic subscription on the client side
        // exists. The assumption is that the client should have in memory the
        // Channel created for the TopicSubscriber once the server has sent
        // an ack response to the initial subscribe request.
        SubscribeResponseHandler subscribeResponseHandler =
            getSubscribeResponseHandler(topicSubscriber);
        if (null == subscribeResponseHandler ||
            !subscribeResponseHandler.hasSubscription(topicSubscriber)) {
            logger.error("Client is not yet subscribed to {}.", topicSubscriber);
            throw new ClientNotSubscribedException("Client is not yet subscribed to "
                                                   + topicSubscriber);
        }

        MessageHandler existedMsgHandler = topicSubscriber2MessageHandler.get(topicSubscriber);
        if (restart) {
            // restart using existing msg handler 
            messageHandler = existedMsgHandler;
        } else {
            // some has started delivery but not stop it
            if (null != existedMsgHandler) {
                throw new AlreadyStartDeliveryException("A message handler has been started for topic subscriber " + topicSubscriber);
            }
            if (messageHandler != null) {
                if (null != topicSubscriber2MessageHandler.putIfAbsent(topicSubscriber, messageHandler)) {
                    throw new AlreadyStartDeliveryException("Someone is also starting delivery for topic subscriber " + topicSubscriber);
                }
            }
        }

        // tell subscribe response handler to start delivering messages for topicSubscriber
        subscribeResponseHandler.startDelivery(topicSubscriber, messageHandler);
    }

    public void stopDelivery(TopicSubscriber topicSubscriber)
    throws ClientNotSubscribedException {
        // Make sure we know that this topic subscription on the client side
        // exists. The assumption is that the client should have in memory the
        // Channel created for the TopicSubscriber once the server has sent
        // an ack response to the initial subscribe request.
        SubscribeResponseHandler subscribeResponseHandler =
            getSubscribeResponseHandler(topicSubscriber);
        if (null == subscribeResponseHandler ||
            !subscribeResponseHandler.hasSubscription(topicSubscriber)) {
            logger.error("Client is not yet subscribed to {}.", topicSubscriber);
            throw new ClientNotSubscribedException("Client is not yet subscribed to "
                                                   + topicSubscriber);
        }

        // tell subscribe response handler to stop delivering messages for a given topic subscriber
        topicSubscriber2MessageHandler.remove(topicSubscriber);
        subscribeResponseHandler.stopDelivery(topicSubscriber);
    }
                            

    @Override
    public void asyncCloseSubscription(final TopicSubscriber topicSubscriber,
                                       final Callback<ResponseBody> callback,
                                       final Object context) {
        HChannel hChannel = topicSubscriber2Channel.removeChannel(topicSubscriber);
        if (null == hChannel) {
            logger.warn("Trying to close a subscription when we don't have a subscribe channel cached for {}",
                        topicSubscriber);
            callback.operationFinished(context, (ResponseBody)null);
            return;
        }

        Channel channel = hChannel.getChannel();
        if (null == channel) {
            callback.operationFinished(context, (ResponseBody)null);
            return;
        }

        try {
            HChannelImpl.getHChannelHandlerFromChannel(channel).closeExplicitly();
        } catch (NoResponseHandlerException nrhe) {
            logger.warn("No Channel Handler found when closing {}'s channel {}.",
                        channel, topicSubscriber);
        }
        ChannelFuture future = channel.close();
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    logger.error("Failed to close the subscription channel for {}",
                                 topicSubscriber);
                    callback.operationFailed(context, new ServiceDownException(
                        "Failed to close the subscription channel for " + topicSubscriber));
                } else {
                    callback.operationFinished(context, (ResponseBody)null);
                }
            }
        });
    }

    @Override
    protected void checkTimeoutRequestsOnSubscriptionChannels() {
        // timeout task may be started before constructing topicSubscriber2Channel
        if (null == topicSubscriber2Channel) {
            return;
        }
        for (HChannel channel : topicSubscriber2Channel.getChannels()) {
            try {
                HChannelHandler channelHandler =
                    HChannelImpl.getHChannelHandlerFromChannel(channel.getChannel());
                channelHandler.checkTimeoutRequests();
            } catch (NoResponseHandlerException nrhe) {
                continue;
            }
        }
    }

    @Override
    protected void closeSubscriptionChannels() {
        topicSubscriber2Channel.close();
    }
}
