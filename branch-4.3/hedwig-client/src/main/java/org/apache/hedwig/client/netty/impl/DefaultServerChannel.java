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
package org.apache.hedwig.client.netty.impl;

import java.net.InetSocketAddress;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import static org.apache.hedwig.util.VarArgs.va;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle requests sent to default hub server. <b>DefaultServerChannel</b> would not
 * be used as a channel to send requests directly. It just takes the responsibility to
 * connect to the default server. After the underlying netty channel is established,
 * it would call {@link HChannelManager#submitOpThruChannel()} to send requests thru
 * the underlying netty channel.
 */
class DefaultServerChannel extends HChannelImpl {

    private static Logger logger = LoggerFactory.getLogger(DefaultServerChannel.class);

    DefaultServerChannel(InetSocketAddress host, AbstractHChannelManager channelManager) {
        super(host, channelManager);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[DefaultServer: ").append(host).append("]");
        return sb.toString();
    }

    @Override
    public void submitOp(final PubSubData pubSubData) {
        // for each pub/sub request sent to default hub server
        // we would establish a fresh connection for it
        ClientChannelPipelineFactory pipelineFactory;
        if (OperationType.PUBLISH.equals(pubSubData.operationType) ||
            OperationType.UNSUBSCRIBE.equals(pubSubData.operationType)) {
            pipelineFactory = channelManager.getNonSubscriptionChannelPipelineFactory();
        } else {
            pipelineFactory = channelManager.getSubscriptionChannelPipelineFactory();
        }
        ChannelFuture future = connect(host, pipelineFactory);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                // If the channel has been closed, there is no need to proceed with any callback
                // logic here.
                if (closed) {
                    future.getChannel().close();
                    return;
                }

                // Check if the connection to the server was done successfully.
                if (!future.isSuccess()) {
                    logger.error("Error connecting to host {}.", host);
                    future.getChannel().close();

                    retryOrFailOp(pubSubData);
                    // Finished with failure logic so just return.
                    return;
                }
                logger.debug("Connected to host {} for pubSubData: {}",
                             va(host, pubSubData));
                channelManager.submitOpThruChannel(pubSubData, future.getChannel());
            }
        });
    }

}
