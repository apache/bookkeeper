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
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

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
import org.apache.hedwig.client.netty.impl.HChannelImpl;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.filter.ClientMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.util.Callback;
import static org.apache.hedwig.util.VarArgs.va;

public class SimpleSubscribeResponseHandler extends SubscribeResponseHandler {

    private static Logger logger = LoggerFactory.getLogger(SimpleSubscribeResponseHandler.class);

    // Member variables used when this ResponseHandler is for a Subscribe
    // channel. We need to be able to consume messages sent back to us from
    // the server, and to also recreate the Channel connection if it ever goes
    // down. For that, we need to store the original PubSubData for the
    // subscribe request, and also the MessageHandler that was registered when
    // delivery of messages started for the subscription.
    private volatile PubSubData origSubData;
    private volatile TopicSubscriber origTopicSubscriber;
    private SubscriptionPreferences preferences;
    private Channel subscribeChannel;
    private MessageHandler messageHandler;
    // Counter for the number of consumed messages so far to buffer up before we
    // send the Consume message back to the server along with the last/largest
    // message seq ID seen so far in that batch.
    private int numConsumedMessagesInBuffer = 0;
    private MessageSeqId lastMessageSeqId;
    // Queue used for subscribes when the MessageHandler hasn't been registered
    // yet but we've already received subscription messages from the server.
    // This will be lazily created as needed.
    private Queue<Message> subscribeMsgQueue;
    // Set to store all of the outstanding subscribed messages that are pending
    // to be consumed by the client app's MessageHandler. If this ever grows too
    // big (e.g. problem at the client end for message consumption), we can
    // throttle things by temporarily setting the Subscribe Netty Channel
    // to not be readable. When the Set has shrunk sufficiently, we can turn the
    // channel back on to read new messages.
    private Set<Message> outstandingMsgSet;

    private SimpleHChannelManager sChannelManager;

    protected SimpleSubscribeResponseHandler(ClientConfiguration cfg,
                                             HChannelManager channelManager) {
        super(cfg, channelManager);
        sChannelManager = (SimpleHChannelManager) channelManager;
        origTopicSubscriber = null;
    }

    protected HChannelManager getHChannelManager() {
        return this.sChannelManager;
    }

    protected ClientConfiguration getConfiguration() {
        return cfg;
    }

    protected MessageHandler getMessageHandler() {
        return messageHandler;
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

        if (logger.isDebugEnabled()) {
            logger.debug("Handling a Subscribe response: {}, pubSubData: {}, host: {}.",
                         va(response, pubSubData, NetUtils.getHostFromChannel(channel)));
        }
        switch (response.getStatusCode()) {
        case SUCCESS:
            // Store the original PubSubData used to create this successful
            // Subscribe request.
            origSubData = pubSubData;
            origTopicSubscriber = new TopicSubscriber(pubSubData.topic,
                                                      pubSubData.subscriberId);
            synchronized(this) {
                // For successful Subscribe requests, store this Channel locally
                // and set it to not be readable initially.
                // This way we won't be delivering messages for this topic
                // subscription until the client explicitly says so.
                subscribeChannel = channel;
                subscribeChannel.setReadable(false);
                if (response.hasResponseBody()) {
                    ResponseBody respBody = response.getResponseBody(); 
                    if (respBody.hasSubscribeResponse()) {
                        SubscribeResponse resp = respBody.getSubscribeResponse();
                        if (resp.hasPreferences()) {
                            preferences = resp.getPreferences();
                            if (logger.isDebugEnabled()) {
                                logger.debug("Receive subscription preferences for {} : {}",
                                             va(origTopicSubscriber,
                                                SubscriptionStateUtils.toString(preferences)));
                            }
                        }
                    }
                }

                // Store the mapping for the TopicSubscriber to the Channel.
                // This is so we can control the starting and stopping of
                // message deliveries from the server on that Channel. Store
                // this only on a successful ack response from the server.
                sChannelManager.storeSubscriptionChannel(origTopicSubscriber,
                                                                    channel);

                // Lazily create the Set (from a concurrent hashmap) to keep track
                // of outstanding Messages to be consumed by the client app. At this
                // stage, delivery for that topic hasn't started yet so creation of
                // this Set should be thread safe. We'll create the Set with an initial
                // capacity equal to the configured parameter for the maximum number of
                // outstanding messages to allow. The load factor will be set to
                // 1.0f which means we'll only rehash and allocate more space if
                // we ever exceed the initial capacity. That should be okay
                // because when that happens, things are slow already and piling
                // up on the client app side to consume messages.
                outstandingMsgSet = Collections.newSetFromMap(
                        new ConcurrentHashMap<Message,Boolean>(
                                cfg.getMaximumOutstandingMessages(), 1.0f));
            }
            // Response was success so invoke the callback's operationFinished
            // method.
            pubSubData.getCallback().operationFinished(pubSubData.context, null);
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
            logger.error("Unexpected error response from server for PubSubResponse: {}", response);
            pubSubData.getCallback().operationFailed(pubSubData.context, new ServiceDownException(
                                                     "Server responded with a status code of: " + response.getStatusCode()));
            break;
        }
    }

    @Override
    public void handleSubscribeMessage(PubSubResponse response) {
        if (logger.isDebugEnabled()) {
            logger.debug("Handling a Subscribe message in response: {}, {}",
                         va(response, origTopicSubscriber));
        }
        Message message = response.getMessage();

        synchronized (this) {
            // Consume the message asynchronously that the client is subscribed
            // to. Do this only if delivery for the subscription has started and
            // a MessageHandler has been registered for the TopicSubscriber.
            if (messageHandler != null) {
                asyncMessageDeliver(origTopicSubscriber, message);
            } else {
                // MessageHandler has not yet been registered so queue up these
                // messages for the Topic Subscription. Make the initial lazy
                // creation of the message queue thread safe just so we don't
                // run into a race condition where two simultaneous threads process
                // a received message and both try to create a new instance of
                // the message queue. Performance overhead should be okay
                // because the delivery of the topic has not even started yet
                // so these messages are not consumed and just buffered up here.
                if (subscribeMsgQueue == null)
                    subscribeMsgQueue = new LinkedList<Message>();
                if (logger.isDebugEnabled()) {
                    logger
                    .debug("Message has arrived but Subscribe channel does not have a registered MessageHandler yet so queueing up the message: {}",
                           message);
                }
                subscribeMsgQueue.add(message);
            }
        }
    }

    @Override
    protected void asyncMessageDeliver(TopicSubscriber topicSubscriber,
                                       Message message) {
        if (logger.isDebugEnabled()) {
            logger.debug("Call the client app's MessageHandler asynchronously to deliver the message {} to {}",
                         va(message, topicSubscriber));
        }
        synchronized (this) { 
            // Add this "pending to be consumed" message to the outstandingMsgSet.
            outstandingMsgSet.add(message);
            // Check if we've exceeded the max size for the outstanding message set.
            if (outstandingMsgSet.size() >= cfg.getMaximumOutstandingMessages()
                    && subscribeChannel.isReadable()) {
                // Too many outstanding messages so throttle it by setting the Netty
                // Channel to not be readable.
                if (logger.isDebugEnabled()) {
                    logger.debug("Too many outstanding messages ({}) so throttling the subscribe netty Channel",
                                 outstandingMsgSet.size());
                }
                subscribeChannel.setReadable(false);
            }
        }
        MessageConsumeData messageConsumeData =
            new MessageConsumeData(topicSubscriber, message);
        messageHandler.deliver(topicSubscriber.getTopic(), topicSubscriber.getSubscriberId(),
                               message, sChannelManager.getConsumeCallback(),
                               messageConsumeData);
    }

    @Override
    protected synchronized void messageConsumed(TopicSubscriber topicSubscriber,
                                                Message message) {
        logger.debug("Message has been successfully consumed by the client app for message:  {}, {}",
                     message, topicSubscriber);
        // Update the consumed messages buffer variables
        if (cfg.isAutoSendConsumeMessageEnabled()) {
            // Update these variables only if we are auto-sending consume
            // messages to the server. Otherwise the onus is on the client app
            // to call the Subscriber consume API to let the server know which
            // messages it has successfully consumed.
            numConsumedMessagesInBuffer++;
            lastMessageSeqId = message.getMsgId();
        }
        // Remove this consumed message from the outstanding Message Set.
        outstandingMsgSet.remove(message);

        // For consume response to server, there is a config param on how many
        // messages to consume and buffer up before sending the consume request.
        // We just need to keep a count of the number of messages consumed
        // and the largest/latest msg ID seen so far in this batch. Messages
        // should be delivered in order and without gaps. Do this only if
        // auto-sending of consume messages is enabled.
        if (cfg.isAutoSendConsumeMessageEnabled()
                && numConsumedMessagesInBuffer >= cfg.getConsumedMessagesBufferSize()) {
            // Send the consume request and reset the consumed messages buffer
            // variables. We will use the same Channel created from the
            // subscribe request for the TopicSubscriber.
            if (logger.isDebugEnabled()) {
                logger
                .debug("Consumed message buffer limit reached so send the Consume Request to the server with lastMessageSeqId: {}",
                       lastMessageSeqId);
            }
            consume(topicSubscriber, lastMessageSeqId);
            numConsumedMessagesInBuffer = 0;
            lastMessageSeqId = null;
        }

        // Check if we throttled message consumption previously when the
        // outstanding message limit was reached. For now, only turn the
        // delivery back on if there are no more outstanding messages to
        // consume. We could make this a configurable parameter if needed.
        if (!subscribeChannel.isReadable() && outstandingMsgSet.size() == 0) {
            if (logger.isDebugEnabled())
                logger
                .debug("Message consumption has caught up so okay to turn off throttling of messages on the subscribe channel for {}",
                       topicSubscriber);
            subscribeChannel.setReadable(true);
        }
    }

    @Override
    public void startDelivery(final TopicSubscriber topicSubscriber,
                              MessageHandler messageHandler)
    throws ClientNotSubscribedException, AlreadyStartDeliveryException {
        if (logger.isDebugEnabled()) {
            logger.debug("Start delivering message for {} using message handler {}",
                         va(topicSubscriber, messageHandler));
        }
        if (!hasSubscription(topicSubscriber)) {
            throw new ClientNotSubscribedException("Client is not yet subscribed to " + topicSubscriber);
        }
        synchronized (this) {
            if (null != this.messageHandler) {
                    throw new AlreadyStartDeliveryException("A message handler " + this.messageHandler
                        + " has been started for " + topicSubscriber);
            }
            // instantiante a message handler
            if (null != messageHandler &&
                messageHandler instanceof FilterableMessageHandler) {
                FilterableMessageHandler filterMsgHandler =
                    (FilterableMessageHandler) messageHandler;
                // pass subscription preferences to message filter
                if (null == preferences) {
                    // no preferences means talking to an old version hub server
                    logger.warn("Start delivering messages with filter but no subscription "
                              + "preferences found. It might due to talking to an old version"
                              + " hub server.");
                    // use the original message handler.
                    messageHandler = filterMsgHandler.getMessageHandler();
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Start delivering messages with filter on {}, preferences: {}",
                                     va(topicSubscriber,
                                        SubscriptionStateUtils.toString(preferences)));
                    }
                    ClientMessageFilter msgFilter = filterMsgHandler.getMessageFilter();
                    msgFilter.setSubscriptionPreferences(
                        topicSubscriber.getTopic(), topicSubscriber.getSubscriberId(),
                        preferences);
                }
            }

            this.messageHandler = messageHandler;
            // Once the MessageHandler is registered, see if we have any queued up
            // subscription messages sent to us already from the server. If so,
            // consume those first. Do this only if the MessageHandler registered is
            // not null (since that would be the HedwigSubscriber.stopDelivery
            // call).
            if (messageHandler != null && subscribeMsgQueue != null && subscribeMsgQueue.size() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Consuming {} queued up messages for {}",
                                 va(subscribeMsgQueue.size(), topicSubscriber));
                }
                for (Message message : subscribeMsgQueue) {
                    asyncMessageDeliver(topicSubscriber, message);
                }
                // Now we can remove the queued up messages since they are all
                // consumed.
                subscribeMsgQueue.clear();
            }
            // Now make the TopicSubscriber Channel readable (it is set to not be
            // readable when the initial subscription is done). Note that this is an
            // asynchronous call. If this fails (not likely), the futureListener
            // will just log an error message for now.
            ChannelFuture future = subscribeChannel.setReadable(true);
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
    }

    @Override
    public void stopDelivery(final TopicSubscriber topicSubscriber)
    throws ClientNotSubscribedException {
        if (logger.isDebugEnabled()) {
            logger.debug("Stop delivering messages for {}", topicSubscriber);
        }

        if (!hasSubscription(topicSubscriber)) {
            throw new ClientNotSubscribedException("Client is not yet subscribed to " + topicSubscriber);
        }

        synchronized (this) {
            this.messageHandler = null;
            // Now make the TopicSubscriber channel not-readable. This will buffer
            // up messages if any are sent from the server. Note that this is an
            // asynchronous call. If this fails (not likely), the futureListener
            // will just log an error message for now.
            ChannelFuture future = subscribeChannel.setReadable(false);
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

    @Override
    public boolean hasSubscription(TopicSubscriber topicSubscriber) {
        if (null == origTopicSubscriber) {
            return false;
        } else {
            return origTopicSubscriber.equals(topicSubscriber);
        }
    }

    @Override
    public void asyncCloseSubscription(final TopicSubscriber topicSubscriber,
                                       final Callback<ResponseBody> callback,
                                       final Object context) {
        // nothing to do just clear status
        // channel manager takes the responsibility to close the channel
        callback.operationFinished(context, (ResponseBody)null);
    }

    @Override
    public synchronized  void consume(final TopicSubscriber topicSubscriber,
                                      final MessageSeqId messageSeqId) {
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
                         va(NetUtils.getHostFromChannel(subscribeChannel),
                            messageSeqId, topicSubscriber));
        }
        ChannelFuture future = subscribeChannel.write(pubsubRequestBuilder.build());
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    logger.error("Error writing a Consume request to host: {} with messageSeqId: {} for {}",
                                 va(NetUtils.getHostFromChannel(subscribeChannel),
                                    messageSeqId, topicSubscriber));
                }
            }
        });
    }

    @Override
    public void onChannelDisconnected(InetSocketAddress host,
                                      Channel channel) {
        if (origTopicSubscriber == null) {
            return;
        }
        sChannelManager.clearHostForTopic(origTopicSubscriber.getTopic(), host);
        // clear subscription status
        sChannelManager.asyncCloseSubscription(origTopicSubscriber, new Callback<ResponseBody>() {

            @Override
            public void operationFinished(Object ctx, ResponseBody result) {
                finish();
            }

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                finish();
            }

            private void finish() {
                // Since the connection to the server host that was responsible
                // for the topic died, we are not sure about the state of that
                // server. Resend the original subscribe request data to the default
                // server host/VIP. Also clear out all of the servers we've
                // contacted or attempted to from this request as we are starting a
                // "fresh" subscribe request.
                origSubData.clearServersList();
                // do resubscribe if the subscription enables it
                if (origSubData.options.getEnableResubscribe()) {
                    // Set a new type of VoidCallback for this async call. We need this
                    // hook so after the subscribe reconnect has completed, delivery for
                    // that topic subscriber should also be restarted (if it was that
                    // case before the channel disconnect).
                    final long retryWaitTime = cfg.getSubscribeReconnectRetryWaitTime();
                    SubscribeReconnectCallback reconnectCb =
                        new SubscribeReconnectCallback(origTopicSubscriber,
                                                       origSubData,
                                                       sChannelManager,
                                                       retryWaitTime);
                    origSubData.setCallback(reconnectCb);
                    origSubData.context = null;
                    // Clear the shouldClaim flag
                    origSubData.shouldClaim = false;
                    logger.debug("Reconnect {}'s subscription channel with origSubData {}",
                                 origTopicSubscriber, origSubData);
                    sChannelManager.submitOpToDefaultServer(origSubData);
                } else {
                    logger.info("Subscription channel for ({}) is disconnected.",
                                origTopicSubscriber);
                    sChannelManager.getSubscriptionEventEmitter().emitSubscriptionEvent(
                        origSubData.topic, origSubData.subscriberId, SubscriptionEvent.TOPIC_MOVED);
                }
            }
        }, null);
    }

}
