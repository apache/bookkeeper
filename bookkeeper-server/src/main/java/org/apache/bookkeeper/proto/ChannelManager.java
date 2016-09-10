/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.proto;

import java.io.IOException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;

/**
 * Manages the lifycycle of a communication Channel
 * @author enrico.olivelli
 */
public abstract class ChannelManager {

    /**
     * Boots the Channel
     * @param conf Bookie Configuration
     * @param channelPipelineFactory Netty Pipeline Factory
     * @param bookieAddress The actual address to listen on
     * @return the channel which is listening for incoming connections
     * @throws IOException 
     */
    public abstract Channel start(ServerConfiguration conf, ChannelPipelineFactory channelPipelineFactory) throws IOException;

    /**
     * Releases all resources
     */
    public abstract void close();
}
