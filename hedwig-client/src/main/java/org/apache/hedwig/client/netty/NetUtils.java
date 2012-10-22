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

import org.jboss.netty.channel.Channel;

import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.protocol.PubSubProtocol.CloseSubscriptionRequest;
import org.apache.hedwig.protocol.PubSubProtocol.ConsumeRequest;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.PublishRequest;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.protocol.PubSubProtocol.ProtocolVersion;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.protocol.PubSubProtocol.UnsubscribeRequest;

/**
 * Utilities for network operations.
 */
public class NetUtils {

    /**
     * Helper static method to get the String Hostname:Port from a netty
     * Channel. Assumption is that the netty Channel was originally created with
     * an InetSocketAddress. This is true with the Hedwig netty implementation.
     *
     * @param channel
     *            Netty channel to extract the hostname and port from.
     * @return String representation of the Hostname:Port from the Netty Channel
     */
    public static InetSocketAddress getHostFromChannel(Channel channel) {
        return (InetSocketAddress) channel.getRemoteAddress();
    }

    /**
     * This is a helper method to build the actual pub/sub message.
     *
     * @param txnId
     *            Transaction Id.
     * @param pubSubData
     *            Publish call's data wrapper object.
     * @return pub sub request to send
     */
    public static PubSubRequest.Builder buildPubSubRequest(long txnId,
                                                           PubSubData pubSubData) {
        // Create a PubSubRequest
        PubSubRequest.Builder pubsubRequestBuilder = PubSubRequest.newBuilder();
        pubsubRequestBuilder.setProtocolVersion(ProtocolVersion.VERSION_ONE);
        pubsubRequestBuilder.setType(pubSubData.operationType);
        // for consume request, we don't need to care about tried servers list
        if (OperationType.CONSUME != pubSubData.operationType) {
            if (pubSubData.triedServers != null && pubSubData.triedServers.size() > 0) {
                pubsubRequestBuilder.addAllTriedServers(pubSubData.triedServers);
            }
        }
        pubsubRequestBuilder.setTxnId(txnId);
        pubsubRequestBuilder.setShouldClaim(pubSubData.shouldClaim);
        pubsubRequestBuilder.setTopic(pubSubData.topic);

        switch (pubSubData.operationType) {
        case PUBLISH:
            // Set the PublishRequest into the outer PubSubRequest
            pubsubRequestBuilder.setPublishRequest(buildPublishRequest(pubSubData));
            break;
        case SUBSCRIBE:
            // Set the SubscribeRequest into the outer PubSubRequest
            pubsubRequestBuilder.setSubscribeRequest(buildSubscribeRequest(pubSubData));
            break;
        case UNSUBSCRIBE:
            // Set the UnsubscribeRequest into the outer PubSubRequest
            pubsubRequestBuilder.setUnsubscribeRequest(buildUnsubscribeRequest(pubSubData));
            break;
        case CLOSESUBSCRIPTION:
            // Set the CloseSubscriptionRequest into the outer PubSubRequest
            pubsubRequestBuilder.setCloseSubscriptionRequest(
                buildCloseSubscriptionRequest(pubSubData));
            break;
        }

        // Update the PubSubData with the txnId and the requestWriteTime
        pubSubData.txnId = txnId;
        pubSubData.requestWriteTime = System.currentTimeMillis();

        return pubsubRequestBuilder;
    }

    // build publish request
    private static PublishRequest.Builder buildPublishRequest(PubSubData pubSubData) {
        PublishRequest.Builder publishRequestBuilder = PublishRequest.newBuilder();
        publishRequestBuilder.setMsg(pubSubData.msg);
        return publishRequestBuilder;
    }

    // build subscribe request
    private static SubscribeRequest.Builder buildSubscribeRequest(PubSubData pubSubData) { SubscribeRequest.Builder subscribeRequestBuilder = SubscribeRequest.newBuilder();
        subscribeRequestBuilder.setSubscriberId(pubSubData.subscriberId);
        subscribeRequestBuilder.setCreateOrAttach(pubSubData.options.getCreateOrAttach());
        subscribeRequestBuilder.setForceAttach(pubSubData.options.getForceAttach());
        // For now, all subscribes should wait for all cross-regional
        // subscriptions to be established before returning.
        subscribeRequestBuilder.setSynchronous(true);
        // set subscription preferences
        SubscriptionPreferences.Builder preferencesBuilder =
            options2Preferences(pubSubData.options);
        // backward compatable with 4.1.0
        if (preferencesBuilder.hasMessageBound()) {
            subscribeRequestBuilder.setMessageBound(preferencesBuilder.getMessageBound());
        } 
        subscribeRequestBuilder.setPreferences(preferencesBuilder);
        return subscribeRequestBuilder;
    }

    // build unsubscribe request
    private static UnsubscribeRequest.Builder buildUnsubscribeRequest(PubSubData pubSubData) {
        // Create the UnSubscribeRequest
        UnsubscribeRequest.Builder unsubscribeRequestBuilder = UnsubscribeRequest.newBuilder();
        unsubscribeRequestBuilder.setSubscriberId(pubSubData.subscriberId);
        return unsubscribeRequestBuilder;
    }

    // build closesubscription request
    private static CloseSubscriptionRequest.Builder
        buildCloseSubscriptionRequest(PubSubData pubSubData) {
        // Create the CloseSubscriptionRequest
        CloseSubscriptionRequest.Builder closeSubscriptionRequestBuilder =
            CloseSubscriptionRequest.newBuilder();
        closeSubscriptionRequestBuilder.setSubscriberId(pubSubData.subscriberId);
        return closeSubscriptionRequestBuilder;
    }

    /**
     * Build consume request
     *
     * @param txnId
     *          Transaction Id.
     * @param topicSubscriber
     *          Topic Subscriber.
     * @param messageSeqId
     *          Message Seq Id.
     * @return pub/sub request.
     */
    public static PubSubRequest.Builder buildConsumeRequest(long txnId,
                                                            TopicSubscriber topicSubscriber,
                                                            MessageSeqId messageSeqId) {
        // Create a PubSubRequest
        PubSubRequest.Builder pubsubRequestBuilder = PubSubRequest.newBuilder();
        pubsubRequestBuilder.setProtocolVersion(ProtocolVersion.VERSION_ONE);
        pubsubRequestBuilder.setType(OperationType.CONSUME);

        pubsubRequestBuilder.setTxnId(txnId);
        pubsubRequestBuilder.setTopic(topicSubscriber.getTopic());

        // Create the ConsumeRequest
        ConsumeRequest.Builder consumeRequestBuilder = ConsumeRequest.newBuilder();
        consumeRequestBuilder.setSubscriberId(topicSubscriber.getSubscriberId());
        consumeRequestBuilder.setMsgId(messageSeqId);

        pubsubRequestBuilder.setConsumeRequest(consumeRequestBuilder);

        return pubsubRequestBuilder;
    }

    /**
     * Convert client-side subscription options to subscription preferences
     *
     * @param options
     *          Client-Side subscription options
     * @return subscription preferences
     */
    private static SubscriptionPreferences.Builder options2Preferences(SubscriptionOptions options) {
        // prepare subscription preferences
        SubscriptionPreferences.Builder preferencesBuilder =
            SubscriptionPreferences.newBuilder();

        // set message bound
        if (options.getMessageBound() > 0) {
            preferencesBuilder.setMessageBound(options.getMessageBound());
        }

        // set message filter
        if (options.hasMessageFilter()) {
            preferencesBuilder.setMessageFilter(options.getMessageFilter());
        }

        // set user options
        if (options.hasOptions()) {
            preferencesBuilder.setOptions(options.getOptions());
        }

        // set message window size if set
        if (options.hasMessageWindowSize() && options.getMessageWindowSize() > 0) {
            preferencesBuilder.setMessageWindowSize(options.getMessageWindowSize());
        }

        return preferencesBuilder;
    }

}
