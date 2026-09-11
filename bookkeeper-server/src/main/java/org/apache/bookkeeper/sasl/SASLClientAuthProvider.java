/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.sasl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import javax.security.auth.Subject;
import javax.security.sasl.SaslException;
import lombok.CustomLog;
import org.apache.bookkeeper.auth.AuthCallbacks;
import org.apache.bookkeeper.auth.AuthToken;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.ClientConnectionPeer;

/**
 * SASL Client Authentication Provider.
 */
@CustomLog
public class SASLClientAuthProvider implements ClientAuthProvider {

    private SaslClientState client;
    private final AuthCallbacks.GenericCallback<Void> completeCb;

    SASLClientAuthProvider(ClientConnectionPeer addr, AuthCallbacks.GenericCallback<Void> completeCb,
        Subject subject) {
        this.completeCb = completeCb;
        try {
            SocketAddress remoteAddr = addr.getRemoteAddr();
            String hostname;
            if (remoteAddr instanceof InetSocketAddress) {
                InetSocketAddress inetSocketAddress = (InetSocketAddress) remoteAddr;
                hostname = inetSocketAddress.getHostName();
            } else {
                hostname = InetAddress.getLocalHost().getHostName();
            }
            client = new SaslClientState(hostname, subject);
            log.debug()
                    .attr("client", client)
                    .attr("hostname", hostname)
                    .log("SASLClientAuthProvider Boot");
        } catch (IOException error) {
            log.error().exception(error).log("Error while booting SASL client");
            completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
        }
    }

    @Override
    public void init(AuthCallbacks.GenericCallback<AuthToken> cb) {
        try {
            if (client.hasInitialResponse()) {
                byte[] response = client.evaluateChallenge(new byte[0]);
                cb.operationComplete(BKException.Code.OK, AuthToken.wrap(response));
            } else {
                cb.operationComplete(BKException.Code.OK, AuthToken.wrap(new byte[0]));
            }
        } catch (SaslException err) {
            log.error().exception(err).log("Error on SASL client");
            completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
        }
    }

    @Override
    public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
        if (client.isComplete()) {
            completeCb.operationComplete(BKException.Code.OK, null);
            return;
        }
        try {
            byte[] responseToken = m.getData();
            byte[] response = client.evaluateChallenge(responseToken);
            if (response == null) {
                response = new byte[0];
            }
            cb.operationComplete(BKException.Code.OK, AuthToken.wrap(response));
            if (client.isComplete()) {
                completeCb.operationComplete(BKException.Code.OK, null);
            }
        } catch (SaslException err) {
            log.error().exception(err).log("Error on SASL client");
            completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
        }

    }

}
