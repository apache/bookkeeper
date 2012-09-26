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
package org.apache.hedwig.client.handlers;

import java.net.InetSocketAddress;
import java.util.LinkedList;

import com.google.protobuf.ByteString;

import org.jboss.netty.channel.Channel;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.netty.HChannelManager;
import org.apache.hedwig.client.exceptions.ServerRedirectLoopException;
import org.apache.hedwig.client.exceptions.TooManyServerRedirectsException;
import org.apache.hedwig.client.netty.NetUtils;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.util.HedwigSocketAddress;
import static org.apache.hedwig.util.VarArgs.va;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractResponseHandler {

    private static Logger logger = LoggerFactory.getLogger(AbstractResponseHandler.class);

    protected final ClientConfiguration cfg;
    protected final HChannelManager channelManager;

    protected AbstractResponseHandler(ClientConfiguration cfg,
                                      HChannelManager channelManager) {
        this.cfg = cfg;
        this.channelManager = channelManager;
    }

    /**
     * Logic to handle received response.
     *
     * @param response
     *            PubSubResponse received from hub server.
     * @param pubSubData
     *            PubSubData for the pub/sub request.
     * @param channel
     *            Channel we used to make the request.
     */
    public abstract void handleResponse(PubSubResponse response, PubSubData pubSubData,
                                        Channel channel) throws Exception;

    /**
     * Logic to repost a PubSubRequest when the server responds with a redirect
     * indicating they are not the topic master.
     *
     * @param response
     *            PubSubResponse from the server for the redirect
     * @param pubSubData
     *            PubSubData for the original PubSubRequest made
     * @param channel
     *            Channel Channel we used to make the original PubSubRequest
     * @throws Exception
     *             Throws an exception if there was an error in doing the
     *             redirect repost of the PubSubRequest
     */
    protected void handleRedirectResponse(PubSubResponse response, PubSubData pubSubData,
                                          Channel channel)
            throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Handling a redirect from host: {}, response: {}, pubSubData: {}",
                         va(NetUtils.getHostFromChannel(channel), response, pubSubData));
        }
        // In this case, the PubSub request was done to a server that is not
        // responsible for the topic. First make sure that we haven't
        // exceeded the maximum number of server redirects.
        int curNumServerRedirects = (pubSubData.triedServers == null) ? 0 : pubSubData.triedServers.size();
        if (curNumServerRedirects >= cfg.getMaximumServerRedirects()) {
            // We've already exceeded the maximum number of server redirects
            // so consider this as an error condition for the client.
            // Invoke the operationFailed callback and just return.
            logger.debug("Exceeded the number of server redirects ({}) so error out.",
                         curNumServerRedirects);
            PubSubException exception = new ServiceDownException(
                new TooManyServerRedirectsException("Already reached max number of redirects: "
                                                    + curNumServerRedirects));
            pubSubData.getCallback().operationFailed(pubSubData.context, exception);
            return;
        }

        // We will redirect and try to connect to the correct server
        // stored in the StatusMsg of the response. First store the
        // server that we sent the PubSub request to for the topic.
        ByteString triedServer = ByteString.copyFromUtf8(HedwigSocketAddress.sockAddrStr(
                                                         NetUtils.getHostFromChannel(channel)));
        if (pubSubData.triedServers == null) {
            pubSubData.triedServers = new LinkedList<ByteString>();
        }
        pubSubData.shouldClaim = true;
        pubSubData.triedServers.add(triedServer);

        // Now get the redirected server host (expected format is
        // Hostname:Port:SSLPort) from the server's response message. If one is
        // not given for some reason, then redirect to the default server
        // host/VIP to repost the request.
        String statusMsg = response.getStatusMsg();
        InetSocketAddress redirectedHost;
        boolean redirectToDefaultServer;
        if (statusMsg != null && statusMsg.length() > 0) {
            if (cfg.isSSLEnabled()) {
                redirectedHost = new HedwigSocketAddress(statusMsg).getSSLSocketAddress();
            } else {
                redirectedHost = new HedwigSocketAddress(statusMsg).getSocketAddress();
            }
            redirectToDefaultServer = false;
        } else {
            redirectedHost = cfg.getDefaultServerHost();
            redirectToDefaultServer = true;
        }

        // Make sure the redirected server is not one we've already attempted
        // already before in this PubSub request.
        if (pubSubData.triedServers.contains(ByteString.copyFromUtf8(HedwigSocketAddress.sockAddrStr(redirectedHost)))) {
            logger.error("We've already sent this PubSubRequest before to redirectedHost: {}, pubSubData: {}",
                         va(redirectedHost, pubSubData));
            PubSubException exception = new ServiceDownException(
                new ServerRedirectLoopException("Already made the request before to redirected host: "
                                                + redirectedHost));
            pubSubData.getCallback().operationFailed(pubSubData.context, exception);
            return;
        }

        // submit the pub/sub request to redirected host
        if (redirectToDefaultServer) {
            channelManager.submitOpToDefaultServer(pubSubData);
        } else {
            channelManager.redirectToHost(pubSubData, redirectedHost);
        }
    }

}
