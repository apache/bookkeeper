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

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.util.Callback;
import static org.apache.hedwig.util.VarArgs.va;

public class SubscriptionChannelManager implements ChannelDisconnectListener {

    static Logger logger = LoggerFactory.getLogger(SubscriptionChannelManager.class);

    private static ChannelFutureListener CLOSE_OLD_CHANNEL_LISTENER =
    new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess()) {
                logger.warn("Failed to close old subscription channel.");
            } else {
                logger.debug("Close old subscription channel succeed.");
            }
        }
    };

    final ConcurrentHashMap<TopicSubscriber, Channel> sub2Channel;
    final ConcurrentHashMap<Channel, TopicSubscriber> channel2sub;

    public SubscriptionChannelManager() {
        sub2Channel = new ConcurrentHashMap<TopicSubscriber, Channel>();
        channel2sub = new ConcurrentHashMap<Channel, TopicSubscriber>();
    }

    @Override
    public void channelDisconnected(Channel channel) {
        // Evils of synchronized programming: there is a race between a channel
        // getting disconnected, and us adding it to the maps when a subscribe
        // succeeds
        synchronized (channel) {
            TopicSubscriber topicSub = channel2sub.remove(channel);
            if (topicSub != null) {
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
            if (null != oldChannel) {
                boolean subSuccess = false;
                if (forceAttach) {
                    // it is safe to close old channel here since new channel will be put
                    // in sub2Channel / channel2Sub so there is no race between channel
                    // getting disconnected and it.
                    ChannelFuture future = oldChannel.close();
                    future.addListener(CLOSE_OLD_CHANNEL_LISTENER);
                    logger.info("Subscribe request for ({}) from channel ({}) kills old channel ({}).",
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
            channel2sub.put(channel, topicSub);
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
            if (!channel2sub.remove(channel, topicSub)) {
                logger.warn("Failed to remove subscription ({}) due to it isn't on channel ({}).",
                            va(topicSub, channel));
            }
            if (!sub2Channel.remove(topicSub, channel)) {
                logger.warn("Failed to remove channel ({}) due to it isn't ({})'s channel.",
                            va(channel, topicSub));
            }
        }
    }
}
