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

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import org.apache.zookeeper.server.auth.KerberosName;
import org.slf4j.LoggerFactory;

/**
 * A SASL Client State data object.
 */
public class SaslClientState {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SaslClientState.class);

    private final SaslClient saslClient;
    private final Subject clientSubject;
    private String username;
    private String password;

    public SaslClientState(String serverHostname, Subject subject) throws SaslException {
        String saslServiceName = System.getProperty(SaslConstants.SASL_SERVICE_NAME,
                                                    SaslConstants.SASL_SERVICE_NAME_DEFAULT);
        String serverPrincipal = saslServiceName + "/" + serverHostname;
        this.clientSubject = subject;
        if (clientSubject == null) {
            throw new SaslException("Cannot create JAAS Sujbect for SASL");
        }
        if (clientSubject.getPrincipals().isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Using JAAS/SASL/DIGEST-MD5 auth to connect to {}", serverPrincipal);
            }
            String[] mechs = {"DIGEST-MD5"};
            username = (String) (clientSubject.getPublicCredentials().toArray()[0]);
            password = (String) (clientSubject.getPrivateCredentials().toArray()[0]);
            saslClient = Sasl.createSaslClient(mechs, username, SaslConstants.SASL_BOOKKEEPER_PROTOCOL,
                SaslConstants.SASL_MD5_DUMMY_HOSTNAME, null, new ClientCallbackHandler(password));
        } else { // GSSAPI/Kerberos
            final Object[] principals = clientSubject.getPrincipals().toArray();
            final Principal clientPrincipal = (Principal) principals[0];
            final KerberosName clientKerberosName = new KerberosName(clientPrincipal.getName());
            KerberosName serviceKerberosName = new KerberosName(serverPrincipal + "@" + clientKerberosName.getRealm());
            final String serviceName = serviceKerberosName.getServiceName();
            final String serviceHostname = serviceKerberosName.getHostName();
            final String clientPrincipalName = clientKerberosName.toString();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Using JAAS/SASL/GSSAPI auth to connect to server Principal {}", serverPrincipal);
            }
            try {
                saslClient = Subject.doAs(clientSubject, new PrivilegedExceptionAction<SaslClient>() {
                    @Override
                    public SaslClient run() throws SaslException {
                        String[] mechs = {"GSSAPI"};
                        return Sasl.createSaslClient(mechs, clientPrincipalName, serviceName, serviceHostname, null,
                            new ClientCallbackHandler(null));
                    }
                });
            } catch (PrivilegedActionException err) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("GSSAPI client error", err.getCause());
                }
                throw new SaslException("error while booting GSSAPI client", err.getCause());
            }
        }
        if (saslClient == null) {
            throw new SaslException("Cannot create JVM SASL Client");
        }

    }

    public byte[] evaluateChallenge(final byte[] saslToken) throws SaslException {
        if (saslToken == null) {
            throw new SaslException("saslToken is null");
        }
        if (clientSubject != null) {
            try {
                final byte[] retval = Subject.doAs(clientSubject, new PrivilegedExceptionAction<byte[]>() {
                        @Override
                        public byte[] run() throws SaslException {
                            return saslClient.evaluateChallenge(saslToken);
                        }
                    });
                return retval;
            } catch (PrivilegedActionException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("SASL error", e.getCause());
                }
                throw new SaslException("SASL/JAAS error", e.getCause());
            }
        } else {
            return saslClient.evaluateChallenge(saslToken);
        }
    }

    public boolean hasInitialResponse() {
        return saslClient.hasInitialResponse();
    }

    static class ClientCallbackHandler implements CallbackHandler {

        private String password = null;

        public ClientCallbackHandler(String password) {
            this.password = password;
        }

        @Override
        public void handle(Callback[] callbacks) throws
            UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nc = (NameCallback) callback;
                    nc.setName(nc.getDefaultName());
                } else {
                    if (callback instanceof PasswordCallback) {
                        PasswordCallback pc = (PasswordCallback) callback;
                        if (password != null) {
                            pc.setPassword(this.password.toCharArray());
                        }
                    } else {
                        if (callback instanceof RealmCallback) {
                            RealmCallback rc = (RealmCallback) callback;
                            rc.setText(rc.getDefaultText());
                        } else {
                            if (callback instanceof AuthorizeCallback) {
                                AuthorizeCallback ac = (AuthorizeCallback) callback;
                                String authid = ac.getAuthenticationID();
                                String authzid = ac.getAuthorizationID();
                                if (authid.equals(authzid)) {
                                    ac.setAuthorized(true);
                                } else {
                                    ac.setAuthorized(false);
                                }
                                if (ac.isAuthorized()) {
                                    ac.setAuthorizedID(authzid);
                                }
                            } else {
                                throw new UnsupportedCallbackException(callback, "Unrecognized SASL ClientCallback");
                            }
                        }
                    }
                }
            }
        }
    }

    public boolean isComplete() {
        return saslClient.isComplete();
    }

    public byte[] saslResponse(byte[] saslTokenMessage) {
        try {
            byte[] retval = saslClient.evaluateChallenge(saslTokenMessage);
            return retval;
        } catch (SaslException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("saslResponse: Failed to respond to SASL server's token:", e);
            }
            return null;
        }
    }

}
