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

/**
 * A Netty based Hedwig client implementation.
 *
 * <h3>Components</h3>
 *
 * The netty based implementation contains following components:
 * <ul>
 *   <li>{@link HChannel}: A interface wrapper of netty {@link org.jboss.netty.channel.Channel}
 *       to submit hedwig's {@link org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest}s
 *       to target host.</li>
 *   <li>{@link HChanneHandler}: A wrapper of netty {@link org.jboss.netty.channel.ChannelHandler}
 *       to handle events of its underlying netty channel, such as responses received, channel
 *       disconnected, etc. A {@link HChannelHandler} is bound with a {@link HChannel}.</li>
 *   <li>{@link HChannelManager}: A manager manages all established {@link HChannel}s.
 *       It provides a clean interface for publisher/subscriber to send
 *       {@link org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest}s</li>
 * </ul>
 *
 * <h3>Main Flow</h3>
 *
 * <ul>
 *   <li>{@link HedwigPublisher}/{@link HedwigSubscriber} delegates {@link HChannelManager}
 *       to submit pub/sub requests.</li>
 *   <li>{@link HChannelManager} find the owner hubs, establish a {@link HChannel} to hub servers
 *       and send the requests to them.</li>
 *   <li>{@link HChannelHandler} dispatches responses to target
 *       {@link org.apache.hedwig.client.handlers.AbstractResponseHandler} to process.</li>
 *   <li>{@link HChannelHandler} detects an underlying netty {@link org.jboss.netty.channel.Channel}
 *       disconnected. It calles {@link HChannelManager} to clear cached {@link HChannel} that
 *       it bound with. For non-subscritpion channels, it would fail all pending requests;
 *       For subscription channels, it would fail all pending requests and retry to reconnect
 *       those successful subscriptions.</li>
 * </ul>
 *
 * <h3>HChannel</h3>
 *
 * Two kinds of {@link HChannel}s provided in current implementation. {@link HChannelImpl}
 * provides the ability to multiplex pub/sub requests in an underlying netty
 * {@link org.jboss.netty.channel.Channel}, while {@link DefaultServerChannel} provides the
 * ability to establish a netty channel {@link org.jboss.netty.channel.Channel} for a pub/sub
 * request. After the underlying netty channel is estabilished, it would be converted into
 * a {@link HChannelImpl} by {@link HChannelManager#submitOpThruChannel(pubSubData, channel)}.
 *
 * Although {@link HChannelImpl} provides multiplexing ability, it still could be used for
 * one-channel-per-subscription case, which just sent only one subscribe request thru the
 * underlying channel.
 *
 * <h3>HChannelHandler</h3>
 *
 * {@link HChannelHandler} is generic netty {@link org.jboss.netty.channel.ChannelHandler},
 * which handles events from the underlying channel. A <i>HChannelHandler</i> is bound with
 * a {@link HChannel} as channel pipeplien when the underlying channel is established. It
 * takes the responsibility of dispatching response to target response handler. For a
 * non-subscription channel, it just handles <b>PUBLISH</b> and <b>UNSUBSCRIBE</b> responses.
 * For a subscription channel, it handles <b>SUBSCRIBE</b> response. For consume requests,
 * we treated them in a fire-and-forget way, so they are not need to be handled by any response
 * handler.
 *
 * <h3>HChannelManager</h3>
 *
 * {@link HChannelManager} manages all outstanding connections to target hub servers for a client.
 * Since a subscription channel acts quite different from a non-subscription channel, the basic
 * implementation {@link AbstractHChannelManager} manages non-subscription channels and
 * subscription channels in different channel sets. Currently hedwig client provides
 * {@link SimpleHChannelManager} which manages subscription channels in one-channel-per-subscription
 * way. In future, if we want to multiplex multiple subscriptions in one channel, we just need
 * to provide an multiplexing version of {@link AbstractHChannelManager} which manages channels
 * in multiplexing way, and a multiplexing version of {@link org.apache.hedwig.client.handlers.SubscribeResponseHandler}
 * which handles multiple subscriptions in one channel.
 */
package org.apache.hedwig.client.netty;
