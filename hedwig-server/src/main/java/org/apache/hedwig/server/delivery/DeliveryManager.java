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
package org.apache.hedwig.server.delivery;

import com.google.protobuf.ByteString;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.filter.ServerMessageFilter;
import org.apache.hedwig.util.Callback;

public interface DeliveryManager {
    public void start();

    /**
     * Start serving a given subscription.
     *
     * @param topic
     *          Topic Name
     * @param subscriberId
     *          Subscriber Id
     * @param preferences
     *          Subscription Preferences
     * @param seqIdToStartFrom
     *          Message sequence id starting delivery from.
     * @param endPoint
     *          End point to deliver messages to.
     * @param filter
     *          Message filter used to filter messages before delivery.
     * @param callback
     *          Callback instance.
     * @param ctx
     *          Callback context.
     */
    public void startServingSubscription(ByteString topic, ByteString subscriberId,
                                         SubscriptionPreferences preferences,
                                         MessageSeqId seqIdToStartFrom,
                                         DeliveryEndPoint endPoint,
                                         ServerMessageFilter filter,
                                         Callback<Void> callback, Object ctx);

    /**
     * Stop serving a given subscription.
     *
     * @param topic
     *          Topic Name
     * @param subscriberId
     *          Subscriber Id
     * @param event
     *          Subscription event indicating the reason to stop the subscriber.
     * @param callback
     *          Callback instance.
     * @param ctx
     *          Callback context.
     */
    public void stopServingSubscriber(ByteString topic, ByteString subscriberId,
                                      SubscriptionEvent event,
                                      Callback<Void> callback, Object ctx);

    /**
     * Tell the delivery manager where that a subscriber has consumed
     *
     * @param topic
     *          Topic Name
     * @param subscriberId
     *          Subscriber Id
     * @param consumedSeqId
     *          Max consumed seq id.
     */
    public void messageConsumed(ByteString topic, ByteString subscriberId,
                                MessageSeqId consumedSeqId);

    /**
     * Stop delivery manager
     */
    public void stop();
}
