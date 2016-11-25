/**
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
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;
import org.apache.bookkeeper.auth.AuthToken;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.ConnectionPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SASLClientAuthProvider implements ClientAuthProvider {

    private SaslClientState client;
    private static final Logger LOG = LoggerFactory.getLogger(SASLClientAuthProvider.class);
    private BookkeeperInternalCallbacks.GenericCallback<Void> completeCb;

    SASLClientAuthProvider(ClientConfiguration configuration, ConnectionPeer connection,
        BookkeeperInternalCallbacks.GenericCallback<Void> completeCb) {
        this.completeCb = completeCb;
        try {
            String hostname;
            if (connection.getRemoteAddr() instanceof InetSocketAddress) {
                hostname = ((InetSocketAddress) connection.getRemoteAddr()).getHostName();
            } else {
                hostname = InetAddress.getLocalHost().getHostName();
            }
            boolean systemRole = configuration.getClientRole().equals(ClientConfiguration.CLIENT_ROLE_SYSTEM);
            client = new SaslClientState(hostname, systemRole, configuration);
            LOG.error("SASLClientAuthProvider Boot {} for {}", new Object[]{client, hostname});
        } catch (IOException | LoginException error) {
            LOG.error("Error while booting SASL client", error);
            completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
        }
    }

    @Override
    public void init(BookkeeperInternalCallbacks.GenericCallback<AuthToken> cb) {
        try {
            if (client.hasInitialResponse()) {
                byte[] response = client.evaluateChallenge(new byte[0]);
                cb.operationComplete(BKException.Code.OK, AuthToken.wrap(response));
            } else {
                cb.operationComplete(BKException.Code.OK, AuthToken.wrap(new byte[0]));
            }
        } catch (SaslException err) {
            LOG.error("Error on SASL client", err);
            completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
        }
    }

    @Override
    public void process(AuthToken m, BookkeeperInternalCallbacks.GenericCallback<AuthToken> cb) {
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
            LOG.error("Error on SASL client", err);
            completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
        }

    }

}
