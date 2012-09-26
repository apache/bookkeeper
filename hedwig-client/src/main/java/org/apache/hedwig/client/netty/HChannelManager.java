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
package org.apache.hedwig.client.netty;

import java.net.InetSocketAddress;
import java.util.TimerTask;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.handlers.SubscribeResponseHandler;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.util.Callback;

/**
 * A manager manages 1) all channels established to hub servers,
 * 2) the actions taken by the topic subscribers.
 */
public interface HChannelManager {

    /**
     * Submit a pub/sub request after a given <code>delay</code>.
     *
     * @param op
     *          Pub/Sub Request.
     * @param delay
     *          Delay time in ms.
     */
    public void submitOpAfterDelay(PubSubData op, long delay);

    /**
     * Submit a pub/sub request.
     *
     * @param pubSubData
     *          Pub/Sub Request.
     */
    public void submitOp(PubSubData pubSubData);

    /**
     * Submit a pub/sub request to default server.
     *
     * @param pubSubData
     *           Pub/Sub request.
     */
    public void submitOpToDefaultServer(PubSubData pubSubData);

    /**
     * Submit a pub/sub request to a given host.
     *
     * @param pubSubData
     *          Pub/Sub request.
     * @param host
     *          Given host address.
     */
    public void redirectToHost(PubSubData pubSubData, InetSocketAddress host);

    /**
     * Generate next transaction id for pub/sub request sending thru this manager.
     *
     * @return next transaction id.
     */
    public long nextTxnId();

    /**
     * Schedule a timer task after a given <code>delay</code>.
     *
     * @param task
     *          A timer task
     * @param delay
     *          Delay time in ms.
     */
    public void schedule(TimerTask task, long delay);

    /**
     * Get the subscribe response handler managed the given <code>topicSubscriber</code>.
     *
     * @param topicSubscriber
     *          Topic Subscriber
     * @return subscribe response handler managed it, otherwise return null.
     */
    public SubscribeResponseHandler getSubscribeResponseHandler(
                                    TopicSubscriber topicSubscriber);

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
    public void startDelivery(TopicSubscriber topicSubscriber,
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
    public void stopDelivery(TopicSubscriber topicSubscriber)
    throws ClientNotSubscribedException;

    /**
     * Close the subscription of the given <code>topicSubscriber</code>.
     *
     * @param topicSubscriber
     *          Topic Subscriber
     * @param callback
     *          Callback
     * @param context
     *          Callback context
     */
    public void asyncCloseSubscription(TopicSubscriber topicSubscriber,
                                       Callback<ResponseBody> callback,
                                       Object context);

    /**
     * Return the subscription event emitter to emit subscription events.
     *
     * @return subscription event emitter.
     */
    public SubscriptionEventEmitter getSubscriptionEventEmitter();

    /**
     * Is the channel manager closed.
     *
     * @return true if the channel manager is closed, otherwise return false.
     */
    public boolean isClosed();

    /**
     * Close the channel manager.
     */
    public void close();
}
