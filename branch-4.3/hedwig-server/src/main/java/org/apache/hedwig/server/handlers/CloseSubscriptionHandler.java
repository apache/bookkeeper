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
package org.apache.hedwig.server.handlers;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;

import com.google.protobuf.ByteString;

import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.CloseSubscriptionRequest;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protoextensions.PubSubResponseUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.netty.ServerStats;
import org.apache.hedwig.server.netty.ServerStats.OpStats;
import org.apache.hedwig.server.netty.UmbrellaHandler;
import org.apache.hedwig.server.subscriptions.SubscriptionManager;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.util.Callback;

public class CloseSubscriptionHandler extends BaseHandler {
    SubscriptionManager subMgr;
    DeliveryManager deliveryMgr;
    SubscriptionChannelManager subChannelMgr;
    // op stats
    final OpStats closesubStats;

    public CloseSubscriptionHandler(ServerConfiguration cfg, TopicManager tm,
                                    SubscriptionManager subMgr,
                                    DeliveryManager deliveryMgr,
                                    SubscriptionChannelManager subChannelMgr) {
        super(tm, cfg);
        this.subMgr = subMgr;
        this.deliveryMgr = deliveryMgr;
        this.subChannelMgr = subChannelMgr;
        closesubStats = ServerStats.getInstance().getOpStats(OperationType.CLOSESUBSCRIPTION);
    }

    @Override
    public void handleRequestAtOwner(final PubSubRequest request, final Channel channel) {
        if (!request.hasCloseSubscriptionRequest()) {
            UmbrellaHandler.sendErrorResponseToMalformedRequest(channel, request.getTxnId(),
                    "Missing closesubscription request data");
            closesubStats.incrementFailedOps();
            return;
        }

        final CloseSubscriptionRequest closesubRequest =
                request.getCloseSubscriptionRequest();
        final ByteString topic = request.getTopic();
        final ByteString subscriberId = closesubRequest.getSubscriberId();

        final long requestTime = System.currentTimeMillis();

        subMgr.closeSubscription(topic, subscriberId, new Callback<Void>() {
            @Override
            public void operationFinished(Object ctx, Void result) {
                // we should not close the channel in delivery manager
                // since client waits the response for closeSubscription request
                // client side would close the channel
                deliveryMgr.stopServingSubscriber(topic, subscriberId, null,
                new Callback<Void>() {
                    @Override
                    public void operationFailed(Object ctx, PubSubException exception) {
                        channel.write(PubSubResponseUtils.getResponseForException(exception, request.getTxnId()));
                        closesubStats.incrementFailedOps();
                    }
                    @Override
                    public void operationFinished(Object ctx, Void resultOfOperation) {
                        // remove the topic subscription from subscription channels
                        subChannelMgr.remove(new TopicSubscriber(topic, subscriberId),
                                             channel);
                        channel.write(PubSubResponseUtils.getSuccessResponse(request.getTxnId()));
                        closesubStats.updateLatency(System.currentTimeMillis() - requestTime);
                    }
                }, null);
            }
            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                channel.write(PubSubResponseUtils.getResponseForException(exception, request.getTxnId()));
                closesubStats.incrementFailedOps();
            }
        }, null);
    }
}
