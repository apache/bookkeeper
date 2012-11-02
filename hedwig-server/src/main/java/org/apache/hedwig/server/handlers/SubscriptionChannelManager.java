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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protoextensions.PubSubResponseUtils;
import org.apache.hedwig.util.Callback;
import static org.apache.hedwig.util.VarArgs.va;

public class SubscriptionChannelManager implements ChannelDisconnectListener {

    static Logger logger = LoggerFactory.getLogger(SubscriptionChannelManager.class);

    static class CloseSubscriptionListener implements ChannelFutureListener {

        final TopicSubscriber ts;

        CloseSubscriptionListener(TopicSubscriber topicSubscriber) {
            this.ts = topicSubscriber;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess()) {
                logger.warn("Failed to write response to close old subscription {}.", ts);
            } else {
                logger.debug("Close old subscription {} succeed.", ts);
            }
        }
    };

    final ConcurrentHashMap<TopicSubscriber, Channel> sub2Channel;
    final ConcurrentHashMap<Channel, Set<TopicSubscriber>> channel2sub;

    public SubscriptionChannelManager() {
        sub2Channel = new ConcurrentHashMap<TopicSubscriber, Channel>();
        channel2sub = new ConcurrentHashMap<Channel, Set<TopicSubscriber>>();
    }

    @Override
    public void channelDisconnected(Channel channel) {
        // Evils of synchronized programming: there is a race between a channel
        // getting disconnected, and us adding it to the maps when a subscribe
        // succeeds
        Set<TopicSubscriber> topicSubs;
        synchronized (channel) {
            topicSubs = channel2sub.remove(channel);
        }
        if (topicSubs != null) {
            for (TopicSubscriber topicSub : topicSubs) {
                logger.info("Subscription channel {} for {} is disconnected.",
                            va(channel.getRemoteAddress(), topicSub));
                // remove entry only currently mapped to given value.
                sub2Channel.remove(topicSub, channel);
            }
        }
    }

    public int getNumSubscriptionChannels() {
        return channel2sub.size();
    }

    public int getNumSubscriptions() {
        return sub2Channel.size();
    }

    /**
     * Put <code>topicSub</code> on Channel <code>channel</code>.
     *
     * @param topicSub
     *          Topic Subscription
     * @param channel
     *          Netty channel
     * @param mode
     *          Create or Attach mode
     * @return null succeed, otherwise the old existed channel.
     */
    public Channel put(TopicSubscriber topicSub, Channel channel, boolean forceAttach) {
        // race with channel getting disconnected while we are adding it
        // to the 2 maps
        synchronized (channel) {
            Channel oldChannel = sub2Channel.putIfAbsent(topicSub, channel);
            // if a subscribe request send from same channel,
            // we treated it a success action.
            if (null != oldChannel && !oldChannel.equals(channel)) {
                boolean subSuccess = false;
                if (forceAttach) {
                    // it is safe to close old subscription here since the new subscription
                    // has come from other channel succeed.
                    synchronized (oldChannel) {
                        Set<TopicSubscriber> oldTopicSubs = channel2sub.get(oldChannel);
                        if (null != oldTopicSubs) {
                            if (!oldTopicSubs.remove(topicSub)) {
                                logger.warn("Failed to remove old subscription ({}) due to it isn't on channel ({}).",
                                            va(topicSub, oldChannel));
                            } else if (oldTopicSubs.isEmpty()) {
                                channel2sub.remove(oldChannel);
                            }
                        }
                    }
                    PubSubResponse resp = PubSubResponseUtils.getResponseForSubscriptionEvent(
                        topicSub.getTopic(), topicSub.getSubscriberId(),
                        SubscriptionEvent.SUBSCRIPTION_FORCED_CLOSED
                    );
                    oldChannel.write(resp).addListener(new CloseSubscriptionListener(topicSub));
                    logger.info("Subscribe request for ({}) from channel ({}) closes old subscripiton on channel ({}).",
                                va(topicSub, channel, oldChannel));
                    // try replace the oldChannel
                    // if replace failure, it migth caused because channelDisconnect callback
                    // has removed the old channel.
                    if (!sub2Channel.replace(topicSub, oldChannel, channel)) {
                        // try to add it now.
                        // if add failure, it means other one has obtained the channel
                        oldChannel = sub2Channel.putIfAbsent(topicSub, channel);
                        if (null == oldChannel) {
                            subSuccess = true;
                        }
                    } else {
                        subSuccess = true;
                    }
                }
                if (!subSuccess) {
                    logger.error("Error serving subscribe request for ({}) from ({}) since it already served on ({}).",
                                 va(topicSub, channel, oldChannel));
                    return oldChannel;
                }
            }
            // channel2sub is just a cache, so we can add to it
            // without synchronization
            Set<TopicSubscriber> topicSubs = channel2sub.get(channel);
            if (null == topicSubs) {
                topicSubs = new HashSet<TopicSubscriber>();
                channel2sub.put(channel, topicSubs); 
            }
            topicSubs.add(topicSub);
            return null;
        }
    }

    /**
     * Remove <code>topicSub</code> from Channel <code>channel</code>
     *
     * @param topicSub
     *          Topic Subscription
     * @param channel
     *          Netty channel
     */
    public void remove(TopicSubscriber topicSub, Channel channel) {
        synchronized (channel) {
            Set<TopicSubscriber> topicSubs = channel2sub.get(channel);
            if (null != topicSubs) {
                if (!topicSubs.remove(topicSub)) {
                    logger.warn("Failed to remove subscription ({}) due to it isn't on channel ({}).",
                                va(topicSub, channel));
                } else if (topicSubs.isEmpty()) {
                    channel2sub.remove(channel);
                }
            }
            if (!sub2Channel.remove(topicSub, channel)) {
                logger.warn("Failed to remove channel ({}) due to it isn't ({})'s channel.",
                            va(channel, topicSub));
            }
        }
    }
}
