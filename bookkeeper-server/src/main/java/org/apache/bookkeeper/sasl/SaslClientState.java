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
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.server.auth.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaslClientState {

    private static final Logger LOG = LoggerFactory.getLogger(SaslClientState.class);

    private SaslClient saslClient;
    private Subject clientSubject;
    private String username;
    private String password;

    public SaslClientState(String serverHostname, boolean systemRole,
        ClientConfiguration clientConfiguration) throws SaslException, LoginException {
        String configurationEntry = systemRole
            ? clientConfiguration.getString(SaslConstants.JAAS_AUDITOR_SECTION_NAME, SaslConstants.JAAS_DEFAULT_AUDITOR_SECTION_NAME)
            : clientConfiguration.getString(SaslConstants.JAAS_CLIENT_SECTION_NAME, SaslConstants.JAAS_DEFAULT_CLIENT_SECTION_NAME);

        clientSubject = loginClient(configurationEntry);

        if (clientSubject == null) {
            throw new SaslException("Cannot find section " + configurationEntry + " in JAAS configuration");
        }

        if (clientSubject.getPrincipals().isEmpty()) {
            LOG.info("Using JAAS/SASL/DIGEST-MD5 auth to connect to hostname:" + serverHostname);
            String[] mechs = {"DIGEST-MD5"};
            username = (String) (clientSubject.getPublicCredentials().toArray()[0]);
            password = (String) (clientSubject.getPrivateCredentials().toArray()[0]);
            saslClient = Sasl.createSaslClient(mechs, username, SaslConstants.SASL_BOOKKEEPER_PROTOCOL,
                SaslConstants.SASL_MD5_DUMMY_HOSTNAME, null,
                new ClientCallbackHandler(password));
        } else { // GSSAPI.
            String serverPrincipal = SaslConstants.SASL_BOOKKEEPER_PROTOCOL + "/" + serverHostname;
            final Object[] principals = clientSubject.getPrincipals().toArray();
            // determine client principal from subject.
            final Principal clientPrincipal = (Principal) principals[0];
            final KerberosName clientKerberosName = new KerberosName(clientPrincipal.getName());
            KerberosName serviceKerberosName = new KerberosName(serverPrincipal + "@" + clientKerberosName.getRealm());
            final String serviceName = serviceKerberosName.getServiceName();
            final String serviceHostname = serviceKerberosName.getHostName();
            final String clientPrincipalName = clientKerberosName.toString();
            LOG.info("Using JAAS/SASL/GSSAPI auth to connect to server Principal " + serverPrincipal);
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
                throw new SaslException("error while booting GSSAPI client", err);
            }
        }
        if (saslClient == null) {
            throw new SaslException("Cannot create JVM SASL Client");
        }

    }

    public byte[] evaluateChallenge(final byte[] saslToken) throws SaslException {
        if (saslToken == null) {
            throw new SaslException("saslToken is null.");
        }

        if (clientSubject != null) {
            try {
                final byte[] retval
                    = Subject.doAs(clientSubject, new PrivilegedExceptionAction<byte[]>() {
                        public byte[] run() throws SaslException {
                            return saslClient.evaluateChallenge(saslToken);
                        }
                    });
                return retval;
            } catch (PrivilegedActionException e) {
                LOG.error("SASL/JAAS error", e.getException());
                throw new SaslException("SASL/JAAS error", e.getException());
            }
        } else {
            return saslClient.evaluateChallenge(saslToken);
        }
    }

    private Subject loginClient(String clientSection) throws SaslException, LoginException {
        AppConfigurationEntry[] entries = Configuration.getConfiguration().getAppConfigurationEntry(clientSection);
        if (entries == null) {
            LOG.error("No JAAS Configuration found with section " + clientSection);
            return null;
        }
        try {
            LoginContext loginContext = new LoginContext(clientSection, new ClientCallbackHandler(null));
            loginContext.login();
            LOG.info("Using JAAS Configuration subject: " + loginContext.getSubject());
            return loginContext.getSubject();
        } catch (LoginException error) {
            LOG.error("Error JAAS Configuration subject", error);
            return null;
        }
    }

    public boolean hasInitialResponse() {
        return saslClient.hasInitialResponse();
    }

    private static class ClientCallbackHandler implements CallbackHandler {

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

    /**
     * Respond to server's SASL token.
     *
     * @param saslTokenMessage contains server's SASL token
     * @return client's response SASL token
     */
    public byte[] saslResponse(byte[] saslTokenMessage) {
        try {
            byte[] retval = saslClient.evaluateChallenge(saslTokenMessage);
            return retval;
        } catch (SaslException e) {
            LOG.error("saslResponse: Failed to respond to SASL server's token", e);
            return null;
        }
    }

    private static class SaslClientCallbackHandler implements CallbackHandler {

        private final String userName;

        private final char[] userPassword;

        public SaslClientCallbackHandler(String username, char[] token) {
            this.userName = username;
            this.userPassword = token;
        }

        /**
         * Implementation used to respond to SASL tokens from server.
         *
         * @param callbacks objects that indicate what credential information the server's SaslServer requires from the
         * client.
         * @throws UnsupportedCallbackException
         */
        public void handle(Callback[] callbacks)
            throws UnsupportedCallbackException {
            NameCallback nc = null;
            PasswordCallback pc = null;
            RealmCallback rc = null;
            for (Callback callback : callbacks) {
                if (callback instanceof RealmChoiceCallback) {
                    continue;
                } else if (callback instanceof NameCallback) {
                    nc = (NameCallback) callback;
                } else if (callback instanceof PasswordCallback) {
                    pc = (PasswordCallback) callback;
                } else if (callback instanceof RealmCallback) {
                    rc = (RealmCallback) callback;
                } else {
                    throw new UnsupportedCallbackException(callback,
                        "handle: Unrecognized SASL client callback");
                }
            }
            if (nc != null) {
                nc.setName(userName);
            }
            if (pc != null) {
                pc.setPassword(userPassword);
            }
            if (rc != null) {
                rc.setText(rc.getDefaultText());
            }
        }
    }
}
