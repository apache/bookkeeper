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

import java.util.LinkedList;
import java.util.Queue;

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
import org.apache.hedwig.client.netty.HChannel;
import org.apache.hedwig.client.netty.NetUtils;
import org.apache.hedwig.client.netty.FilterableMessageHandler;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.filter.ClientMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.protoextensions.MessageIdUtils;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import static org.apache.hedwig.util.VarArgs.va;

/**
 * an active subscriber handles subscription actions in a channel.
 */
public class ActiveSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(ActiveSubscriber.class);

    protected final ClientConfiguration cfg;
    protected final AbstractHChannelManager channelManager;

    // Subscriber related variables
    protected final TopicSubscriber topicSubscriber;
    protected final PubSubData op;
    protected final SubscriptionPreferences preferences;

    // the underlying netty channel to send request
    protected final Channel channel;
    protected final HChannel hChannel;

    // Counter for the number of consumed messages so far to buffer up before we
    // send the Consume message back to the server along with the last/largest
    // message seq ID seen so far in that batch.
    private int numConsumedMessagesInBuffer = 0;
    private MessageSeqId lastMessageSeqId = null;

    // Message Handler
    private MessageHandler msgHandler = null;

    // Queue used for subscribes when the MessageHandler hasn't been registered
    // yet but we've already received subscription messages from the server.
    // This will be lazily created as needed.
    private final Queue<Message> msgQueue = new LinkedList<Message>();

    /**
     * Construct an active subscriber instance.
     *
     * @param cfg
     *          Client configuration object.
     * @param channelManager
     *          Channel manager instance.
     * @param ts
     *          Topic subscriber.
     * @param op
     *          Pub/Sub request.
     * @param preferences
     *          Subscription preferences for the subscriber.
     * @param channel
     *          Netty channel the subscriber lived.
     */
    public ActiveSubscriber(ClientConfiguration cfg,
                            AbstractHChannelManager channelManager,
                            TopicSubscriber ts, PubSubData op,
                            SubscriptionPreferences preferences,
                            Channel channel,
                            HChannel hChannel) {
        this.cfg = cfg;
        this.channelManager = channelManager;
        this.topicSubscriber = ts;
        this.op = op;
        this.preferences = preferences;
        this.channel = channel;
        this.hChannel = hChannel;
    }

    /**
     * @return pub/sub request for the subscription.
     */
    public PubSubData getPubSubData() {
        return this.op;
    }

    /**
     * @return topic subscriber id for the active subscriber.
     */
    public TopicSubscriber getTopicSubscriber() {
        return this.topicSubscriber;
    }

    /**
     * Start delivering messages using given message handler.
     *
     * @param messageHandler
     *          Message handler to deliver messages
     * @throws AlreadyStartDeliveryException if someone already started delivery.
     * @throws ClientNotSubscribedException when start delivery before subscribe.
     */
    public synchronized void startDelivery(MessageHandler messageHandler)
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

    /**
     * Stop delivering messages to the subscriber.
     */
    public synchronized void stopDelivery() {
        this.msgHandler = null;
    }

    /**
     * Handle received message.
     *
     * @param message
     *          Received message.
     */
    public synchronized void handleMessage(Message message) {
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

    /**
     * Deliver message to the client.
     *
     * @param message
     *          Message to deliver.
     */
    public synchronized void asyncMessageDeliver(Message message) {
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
        unsafeDeliverMessage(message);
    }

    /**
     * Unsafe version to deliver message to a message handler.
     * Caller need to handle synchronization issue.
     *
     * @param message
     *          Message to deliver.
     */
    protected void unsafeDeliverMessage(Message message) {
        MessageConsumeData messageConsumeData =
            new MessageConsumeData(topicSubscriber, message);
        msgHandler.deliver(topicSubscriber.getTopic(), topicSubscriber.getSubscriberId(),
                           message, channelManager.getConsumeCallback(),
                           messageConsumeData);
    }

    private synchronized boolean updateLastMessageSeqId(MessageSeqId seqId) {
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

    /**
     * Consume a specific message.
     *
     * @param messageSeqId
     *          Message seq id.
     */
    public void consume(final MessageSeqId messageSeqId) {
        PubSubRequest.Builder pubsubRequestBuilder =
            NetUtils.buildConsumeRequest(channelManager.nextTxnId(),
                                         topicSubscriber, messageSeqId);

        // For Consume requests, we will send them from the client in a fire and
        // forget manner. We are not expecting the server to send back an ack
        // response so no need to register this in the ResponseHandler. There
        // are no callbacks to invoke since this isn't a client initiated
        // action. Instead, just have a future listener that will log an error
        // message if there was a problem writing the consume request.
        if (logger.isDebugEnabled()) {
            logger.debug("Writing a Consume request to channel: {} with messageSeqId: {} for {}",
                         va(channel, messageSeqId, topicSubscriber));
        }
        ChannelFuture future = channel.write(pubsubRequestBuilder.build());
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    logger.error("Error writing a Consume request to channel: {} with messageSeqId: {} for {}",
                                 va(channel, messageSeqId, topicSubscriber));
                }
            }
        });
    }

    /**
     * Application acked to consume message.
     *
     * @param message
     *          Message consumed by application.
     */
    public void messageConsumed(Message message) {
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
            if (updateLastMessageSeqId(message.getMsgId())) {
                // Send the consume request and reset the consumed messages buffer
                // variables. We will use the same Channel created from the
                // subscribe request for the TopicSubscriber.
                if (logger.isDebugEnabled()) {
                    logger.debug("Consume message {} when reaching consumed message buffer limit.",
                                 message.getMsgId());
                }
                consume(message.getMsgId());
            }
        }
    }

    /**
     * Resubscribe a subscriber if necessary.
     *
     * @param event
     *          Subscription Event.
     */
    public void resubscribeIfNecessary(SubscriptionEvent event) {
        // clear topic ownership
        if (SubscriptionEvent.TOPIC_MOVED == event) {
            channelManager.clearHostForTopic(topicSubscriber.getTopic(),
                                             NetUtils.getHostFromChannel(channel));
        }
        if (!op.options.getEnableResubscribe()) {
            channelManager.getSubscriptionEventEmitter().emitSubscriptionEvent(
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
                                    channelManager, retryWaitTime);
        op.setCallback(resubscribeCb);
        op.context = null;
        op.setOriginalChannelForResubscribe(hChannel);
        if (logger.isDebugEnabled()) {
            logger.debug("Resubscribe {} with origSubData {}",
                         va(topicSubscriber, op));
        }
        // resubmit the request
        channelManager.submitOp(op);
    }
}
