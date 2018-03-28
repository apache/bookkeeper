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
import java.util.regex.Pattern;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;

import org.apache.bookkeeper.auth.AuthCallbacks;
import org.apache.bookkeeper.auth.AuthToken;
import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieConnectionPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SASL Bookie Authentication Provider.
 */
public class SASLBookieAuthProvider implements BookieAuthProvider {

    private static final Logger LOG = LoggerFactory.getLogger(SASLBookieAuthProvider.class);

    private SaslServerState server;
    private final AuthCallbacks.GenericCallback<Void> completeCb;

    SASLBookieAuthProvider(BookieConnectionPeer addr, AuthCallbacks.GenericCallback<Void> completeCb,
        ServerConfiguration serverConfiguration, Subject subject, Pattern allowedIdsPattern) {
        this.completeCb = completeCb;
        try {
            server = new SaslServerState(serverConfiguration, subject, allowedIdsPattern);
        } catch (IOException | LoginException error) {
            LOG.error("Error while booting SASL server", error);
            completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
        }
    }

    @Override
    public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
        try {
            byte[] clientSideToken = m.getData();
            byte[] response = server.response(clientSideToken);
            if (response != null) {
                cb.operationComplete(BKException.Code.OK, AuthToken.wrap(response));
            }
            if (server.isComplete()) {
                completeCb.operationComplete(BKException.Code.OK, null);
            }
        } catch (SaslException err) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SASL error", err);
            }
            completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
        }

    }

}
