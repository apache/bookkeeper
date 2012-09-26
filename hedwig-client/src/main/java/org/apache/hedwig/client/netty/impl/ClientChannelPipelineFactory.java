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

import java.util.Map;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.ssl.SslHandler;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.handlers.AbstractResponseHandler;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;

public abstract class ClientChannelPipelineFactory implements ChannelPipelineFactory {

    protected ClientConfiguration cfg;
    protected AbstractHChannelManager channelManager;

    public ClientChannelPipelineFactory(ClientConfiguration cfg,
                                        AbstractHChannelManager channelManager) {
        this.cfg = cfg;
        this.channelManager = channelManager;
    }

    protected abstract Map<OperationType, AbstractResponseHandler> createResponseHandlers();

    private HChannelHandler createHChannelHandler() {
        return new HChannelHandler(cfg, channelManager, createResponseHandlers());
    }

    // Retrieve a ChannelPipeline from the factory.
    public ChannelPipeline getPipeline() throws Exception {
        // Create a new ChannelPipline using the factory method from the
        // Channels helper class.
        ChannelPipeline pipeline = Channels.pipeline();
        if (channelManager.getSslFactory() != null) {
            pipeline.addLast("ssl", new SslHandler(channelManager.getSslFactory().getEngine()));
        }
        pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(
                         cfg.getMaximumMessageSize(), 0, 4, 0, 4));
        pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));

        pipeline.addLast("protobufdecoder", new ProtobufDecoder(PubSubProtocol.PubSubResponse.getDefaultInstance()));
        pipeline.addLast("protobufencoder", new ProtobufEncoder());

        pipeline.addLast("responsehandler", createHChannelHandler());
        return pipeline;
    }

}
