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
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.server.subscriptions.InMemorySubscriptionState;
import org.apache.hedwig.util.Callback;

/**
 * Manage subscription data.
 */
public interface SubscriptionDataManager extends Closeable {

    /**
     * Create subscription data.
     *
     * @param topic
     *          Topic name
     * @param subscriberId
     *          Subscriber id
     * @param data 
     *          Subscription data
     * @param callback
     *          Callback when subscription state created.
     *          {@link PubSubException.SubscriptionStateExistsException} is returned when subscription state
     *          existed before.
     * @param ctx
     *          Context of the callback
     */
    public void createSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData data,
                                       Callback<Void> callback, Object ctx);

    /**
     * Whether the metadata manager supports partial update.
     *
     * @return true if the metadata manager supports partial update.
     *         otherwise, return false.
     */
    public boolean isPartialUpdateSupported();

    /**
     * Update subscription data.
     *
     * @param topic
     *          Topic name
     * @param subscriberId
     *          Subscriber id
     * @param dataToUpdate
     *          Subscription data to update. So it is a partial data, which contains
     *          the part of data to update. The implementation should not replace
     *          existing subscription data with <i>dataToUpdate</i> directly.
     *          E.g. if there is only state in it, you should update state only.
     * @param callback
     *          Callback when subscription state updated.
     *          {@link PubSubException.NoSubscriptionStateException} is returned when no subscription state
     *          is found.
     * @param ctx
     *          Context of the callback
     */
    public void updateSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData dataToUpdate,
                                       Callback<Void> callback, Object ctx);

    /**
     * Replace subscription data.
     *
     * @param topic
     *          Topic name
     * @param subscriberId
     *          Subscriber id
     * @param dataToReplace
     *          Subscription data to replace.
     * @param callback
     *          Callback when subscription state updated.
     * @param ctx
     *          Context of the callback
     */
    public void replaceSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData dataToReplace,
                                        Callback<Void> callback, Object ctx);

    /**
     * Remove subscription data.
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
    public void deleteSubscriptionData(ByteString topic, ByteString subscriberId,
                                       Callback<Void> callback, Object ctx);

    /**
     * Read subscription data.
     *
     * @param topic
     *          Topic Name
     * @param subscriberId
     *          Subscriber id
     * @param callback
     *          Callback when subscription data read.
     *          Null is returned when no subscription data is found.
     * @param ctx
     *          Context of the callback
     */
    public void readSubscriptionData(ByteString topic, ByteString subscriberId,
                                     Callback<SubscriptionData> callback, Object ctx);

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
    public void readSubscriptions(ByteString topic, Callback<Map<ByteString, SubscriptionData>> cb,
                                  Object ctx);
}
