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
package org.apache.hedwig.client.netty.impl.simple;

import java.util.HashMap;
import java.util.Map;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.handlers.AbstractResponseHandler;
import org.apache.hedwig.client.handlers.CloseSubscriptionResponseHandler;
import org.apache.hedwig.client.netty.impl.AbstractHChannelManager;
import org.apache.hedwig.client.netty.impl.ClientChannelPipelineFactory;
import org.apache.hedwig.client.netty.impl.HChannelHandler;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;

public class SimpleSubscriptionChannelPipelineFactory extends ClientChannelPipelineFactory {

    public SimpleSubscriptionChannelPipelineFactory(ClientConfiguration cfg,
                                                    SimpleHChannelManager channelManager) {
        super(cfg, channelManager);
    }

    @Override
    protected Map<OperationType, AbstractResponseHandler> createResponseHandlers() {
        Map<OperationType, AbstractResponseHandler> handlers =
            new HashMap<OperationType, AbstractResponseHandler>();
        handlers.put(OperationType.SUBSCRIBE,
                     new SimpleSubscribeResponseHandler(cfg, channelManager));
        handlers.put(OperationType.CLOSESUBSCRIPTION,
                     new CloseSubscriptionResponseHandler(cfg, channelManager));
        return handlers;
    }

}
