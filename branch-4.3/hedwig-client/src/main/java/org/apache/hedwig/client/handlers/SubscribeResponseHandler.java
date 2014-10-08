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
package org.apache.hedwig.client.handlers;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;

import com.google.protobuf.ByteString;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.NoResponseHandlerException;
import org.apache.hedwig.client.netty.HChannelManager;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.util.Callback;

/**
 * An interface provided to manage all subscriptions on a channel.
 *
 * Its responsibility is to handle all subscribe responses received on that channel,
 * clear up subscriptions and retry reconnectin subscriptions when channel disconnected,
 * and handle delivering messages to {@link MessageHandler} and sent consume messages
 * back to hub servers.
 */
public abstract class SubscribeResponseHandler extends AbstractResponseHandler {

    protected SubscribeResponseHandler(ClientConfiguration cfg,
                                       HChannelManager channelManager) {
        super(cfg, channelManager);
    }

    /**
     * Handle Message delivered by the server.
     *
     * @param response
     *          Message received from the server.
     */
    public abstract void handleSubscribeMessage(PubSubResponse response);

    /**
     * Handle a subscription event delivered by the server.
     *
     * @param topic
     *          Topic Name
     * @param subscriberId
     *          Subscriber Id
     * @param event
     *          Subscription Event describes its status
     */
    public abstract void handleSubscriptionEvent(ByteString topic,
                                                 ByteString subscriberId,
                                                 SubscriptionEvent event);

    /**
     * Method called when a message arrives for a subscribe Channel and we want
     * to deliver it asynchronously via the registered MessageHandler (should
     * not be null when called here).
     *
     * @param message
     *            Message from Subscribe Channel we want to consume.
     */
    protected abstract void asyncMessageDeliver(TopicSubscriber topicSubscriber,
                                                Message message);

    /**
     * Method called when the client app's MessageHandler has asynchronously
     * completed consuming a subscribed message sent from the server. The
     * contract with the client app is that messages sent to the handler to be
     * consumed will have the callback response done in the same order. So if we
     * asynchronously call the MessageHandler to consume messages #1-5, that
     * should call the messageConsumed method here via the VoidCallback in the
     * same order. To make this thread safe, since multiple outstanding messages
     * could be consumed by the client app and then called back to here, make
     * this method synchronized.
     *
     * @param topicSubscriber
     *            Topic Subscriber
     * @param message
     *            Message sent from server for topic subscription that has been
     *            consumed by the client.
     */
    protected abstract void messageConsumed(TopicSubscriber topicSubscriber,
                                            Message message);

    /**
     * Start delivering messages for a given topic subscriber.
     *
     * @param topicSubscriber
     *            Topic Subscriber
     * @param messageHandler
     *            MessageHandler to register for this ResponseHandler instance.
     * @throws ClientNotSubscribedException
     *            If the client is not currently subscribed to the topic
     * @throws AlreadyStartDeliveryException
     *            If someone started delivery a message handler before stopping existed one.
     */
    public abstract void startDelivery(TopicSubscriber topicSubscriber,
                                       MessageHandler messageHandler)
    throws ClientNotSubscribedException, AlreadyStartDeliveryException;

    /**
     * Stop delivering messages for a given topic subscriber.
     *
     * @param topicSubscriber
     *            Topic Subscriber
     * @throws ClientNotSubscribedException
     *             If the client is not currently subscribed to the topic
     */
    public abstract void stopDelivery(TopicSubscriber topicSubscriber)
    throws ClientNotSubscribedException;

    /**
     * Whether the given topic subscriber subscribed thru this handler.
     *
     * @param topicSubscriber
     *            Topic Subscriber
     * @return whether the given topic subscriber subscribed thru this handler.
     */
    public abstract boolean hasSubscription(TopicSubscriber topicSubscriber);

    /**
     * Close subscription from this handler.
     *
     * @param topicSubscriber
     *            Topic Subscriber
     * @param callback
     *            Callback when the subscription is closed. 
     * @param context
     *            Callback context.
     */
    public abstract void asyncCloseSubscription(TopicSubscriber topicSubscriber,
                                                Callback<ResponseBody> callback,
                                                Object context);

    /**
     * Consume a given message for given topic subscriber thru this handler.
     *
     * @param topicSubscriber
     *            Topic Subscriber
     */
    public abstract void consume(TopicSubscriber topicSubscriber,
                                 MessageSeqId messageSeqId);

    /**
     * This method is called when the underlying channel is disconnected due to server failure.
     *
     * The implementation should take the responsibility to clear subscriptions and retry
     * reconnecting subscriptions to new hub servers.
     *
     * @param host
     *          Host that channel connected to has disconnected.
     * @param channel
     *          Channel connected to.
     */
    public abstract void onChannelDisconnected(InetSocketAddress host,
                                               Channel channel);
}
