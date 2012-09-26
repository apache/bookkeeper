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
import java.util.LinkedList;

import org.apache.hedwig.client.exceptions.NoResponseHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import com.google.protobuf.ByteString;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.netty.HChannelManager;
import org.apache.hedwig.client.netty.NetUtils;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.util.HedwigSocketAddress;

public class WriteCallback implements ChannelFutureListener {

    private static Logger logger = LoggerFactory.getLogger(WriteCallback.class);

    // Private member variables
    private PubSubData pubSubData;
    private final HChannelManager channelManager;

    // Constructor
    public WriteCallback(PubSubData pubSubData,
                         HChannelManager channelManager) {
        super();
        this.pubSubData = pubSubData;
        this.channelManager = channelManager;
    }

    public void operationComplete(ChannelFuture future) throws Exception {
        // If the client has stopped, there is no need to proceed
        // with any callback logic here.
        if (channelManager.isClosed()) {
            future.getChannel().close();
            return;
        }

        // When the write operation to the server is done, we just need to check
        // if it was successful or not.
        InetSocketAddress host = NetUtils.getHostFromChannel(future.getChannel());
        if (!future.isSuccess()) {
            logger.error("Error writing on channel to host: {}", host);
            // On a write failure for a PubSubRequest, we also want to remove
            // the saved txnId to PubSubData in the ResponseHandler. These
            // requests will not receive an ack response from the server
            // so there is no point storing that information there anymore.
            try {
                HChannelHandler channelHandler = 
                    HChannelImpl.getHChannelHandlerFromChannel(future.getChannel());
                channelHandler.removeTxn(pubSubData.txnId);
                channelHandler.closeExplicitly();
            } catch (NoResponseHandlerException e) {
                // We just couldn't remove the transaction ID's mapping.
                // The handler was null, so this has been reset anyway.
                logger.warn("Could not find response handler to remove txnId mapping to pubsub data. Ignoring.");
            }

            future.getChannel().close();

            // If we were not able to write on the channel to the server host,
            // the host could have died or something is wrong with the channel
            // connection where we can connect to the host, but not write to it.
            ByteString hostString = (host == null) ? null : ByteString.copyFromUtf8(HedwigSocketAddress.sockAddrStr(host));
            if (pubSubData.writeFailedServers != null && pubSubData.writeFailedServers.contains(hostString)) {
                // We've already tried to write to this server previously and
                // failed, so invoke the operationFailed callback.
                logger.error("Error writing to host more than once so just invoke the operationFailed callback!");
                pubSubData.getCallback().operationFailed(pubSubData.context, new ServiceDownException(
                                                        "Error while writing message to server: " + hostString));
            } else {
                logger.debug("Try to send the PubSubRequest again to the default server host/VIP for pubSubData: {}",
                    pubSubData);
                // Keep track of this current server that we failed to write to
                // but retry the request on the default server host/VIP.
                if (pubSubData.writeFailedServers == null)
                    pubSubData.writeFailedServers = new LinkedList<ByteString>();
                pubSubData.writeFailedServers.add(hostString);
                channelManager.submitOpToDefaultServer(pubSubData);
            }
        } else {
            // Now that the write to the server is done, we have to wait for it
            // to respond. The ResponseHandler will take care of the ack
            // response from the server before we can determine if the async
            // PubSub call has really completed successfully or not.
            logger.debug("Successfully wrote to host: {} for pubSubData: {}", host, pubSubData);
        }
    }

}
