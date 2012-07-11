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
package org.apache.hedwig.server.meta;

import java.io.Closeable;
import java.util.Map;

import com.google.protobuf.ByteString;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionState;
import org.apache.hedwig.server.subscriptions.InMemorySubscriptionState;
import org.apache.hedwig.util.Callback;

/**
 * Manage subscription data.
 */
public interface SubscriptionDataManager extends Closeable {

    /**
     * Create subscription state.
     *
     * @param topic
     *          Topic name
     * @param subscriberId
     *          Subscriber id
     * @param state
     *          Subscription state
     * @param callback
     *          Callback when subscription state created.
     *          {@link PubSubException.SubscriptionStateExistsException} is returned when subscription state
     *          existed before.
     * @param ctx
     *          Context of the callback
     */
    public void createSubscriptionState(ByteString topic, ByteString subscriberId, SubscriptionState state,
                                        Callback<Void> callback, Object ctx);

    /**
     * Update subscription state.
     *
     * @param topic
     *          Topic name
     * @param subscriberId
     *          Subscriber id
     * @param state
     *          Subscription state
     * @param callback
     *          Callback when subscription state updated.
     *          {@link PubSubException.NoSubscriptionStateException} is returned when no subscription state
     *          is found.
     * @param ctx
     *          Context of the callback
     */
    public void updateSubscriptionState(ByteString topic, ByteString subscriberId, SubscriptionState state,
                                        Callback<Void> callback, Object ctx);

    /**
     * Remove subscription state.
     *
     * @param topic
     *          Topic name
     * @param subscriberId
     *          Subscriber id
     * @param callback
     *          Callback when subscription state deleted
     *          {@link PubSubException.NoSubscriptionStateException} is returned when no subscription state
     *          is found.
     * @param ctx
     *          Context of the callback
     */
    public void deleteSubscriptionState(ByteString topic, ByteString subscriberId,
                                        Callback<Void> callback, Object ctx);

    /**
     * Read subscription state.
     *
     * @param topic
     *          Topic Name
     * @param subscriberId
     *          Subscriber id
     * @param callback
     *          Callback when subscription state read.
     *          Null is returned when no subscription state is found.
     * @param ctx
     *          Context of the callback
     */
    public void readSubscriptionState(ByteString topic, ByteString subscriberId,
                                      Callback<SubscriptionState> callback, Object ctx);

    /**
     * Read all subscriptions of a topic.
     *
     * @param topic
     *          Topic name
     * @param callback
     *          Callback to return subscriptions
     * @param ctx
     *          Contxt of the callback
     */
    public void readSubscriptions(ByteString topic, Callback<Map<ByteString, InMemorySubscriptionState>> cb,
                                  Object ctx);
}
