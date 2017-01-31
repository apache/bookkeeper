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
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
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
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.zookeeper.server.auth.KerberosName;
import org.slf4j.LoggerFactory;

/**
 * Server side Sasl implementation
 *
 * @author enrico.olivelli
 */
public class SaslServerState {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SaslServerState.class);

    private SaslServer saslServer;

    public SaslServerState(ServerConfiguration configuration) throws IOException, SaslException, LoginException {
        String configurationEntry =
            configuration.getString(SaslConstants.JAAS_BOOKIE_SECTION_NAME, SaslConstants.JAAS_DEFAULT_BOOKIE_SECTION_NAME);
        Subject subject = loginServer(configurationEntry);
        if (subject == null) {
            throw new IOException("no " + SaslConstants.JAAS_DEFAULT_BOOKIE_SECTION_NAME
                + " Section found on JAAS configuration file");
        }
        saslServer = createSaslServer(subject, configurationEntry);
    }

    private SaslServer createSaslServer(final Subject subject, String configurationEntry) throws SaslException, IOException {

        SaslServerCallbackHandler callbackHandler = new SaslServerCallbackHandler(
            Configuration.getConfiguration(), configurationEntry);
        if (subject.getPrincipals().size() > 0) {
            try {
                final Object[] principals = subject.getPrincipals().toArray();
                final Principal servicePrincipal = (Principal) principals[0];

                final String servicePrincipalNameAndHostname = servicePrincipal.getName();
                int indexOf = servicePrincipalNameAndHostname.indexOf("/");
                final String serviceHostnameAndKerbDomain = servicePrincipalNameAndHostname.substring(indexOf + 1,
                    servicePrincipalNameAndHostname.length());
                int indexOfAt = serviceHostnameAndKerbDomain.indexOf("@");

                final String servicePrincipalName, serviceHostname;
                if (indexOf > 0) {
                    servicePrincipalName = servicePrincipalNameAndHostname.substring(0, indexOf);
                    serviceHostname = serviceHostnameAndKerbDomain.substring(0, indexOfAt);
                } else {
                    servicePrincipalName = servicePrincipalNameAndHostname.substring(0, indexOfAt);
                    serviceHostname = null;
                }

                LOG.info("Authentication will use SASL/JAAS/Kerberos, serviceHostname is {},"
                    + " servicePrincipalName is {},"
                    + "JAAS Subject is {}", new Object[]{serviceHostname, servicePrincipalName, subject});

                try {
                    return Subject.doAs(subject, new PrivilegedExceptionAction<SaslServer>() {
                        @Override
                        public SaslServer run() {
                            try {
                                SaslServer saslServer;
                                saslServer = Sasl.createSaslServer("GSSAPI", servicePrincipalName, serviceHostname, null,
                                    callbackHandler);
                                return saslServer;
                            } catch (SaslException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    );
                } catch (PrivilegedActionException e) {
                    LOG.error("Error while booing SASL server", e);
                }
            } catch (IndexOutOfBoundsException e) {
                LOG.error("Error while booing SASL server", e);
            }
            throw new SaslException("error on GSSAPI boot");
        } else {
            LOG.info("Authentication will use SASL/JAAS/DIGEST-MD5");
            SaslServer saslServer = Sasl.createSaslServer("DIGEST-MD5", SaslConstants.SASL_BOOKKEEPER_PROTOCOL,
                SaslConstants.SASL_MD5_DUMMY_HOSTNAME,
                null, callbackHandler);
            return saslServer;
        }
    }

    private Subject loginServer(String sectionName) throws SaslException, LoginException {
        AppConfigurationEntry[] entries = Configuration.getConfiguration().getAppConfigurationEntry(sectionName);
        if (entries == null) {
            LOG.error("JAAS not configured or no {} present in JAAS Configuration file", sectionName);
            return null;
        }
        LoginContext loginContext = new LoginContext(sectionName, new ClientCallbackHandler(null));
        loginContext.login();
        return loginContext.getSubject();

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
        return saslServer.isComplete();
    }

    public String getUserName() {
        return saslServer.getAuthorizationID();
    }

    public byte[] response(byte[] token) throws SaslException {
        try {
            byte[] retval = saslServer.evaluateResponse(token);
            return retval;
        } catch (SaslException e) {
            LOG.error("response: Failed to evaluate client token of length: "
                + token.length + " : " + e);
            throw e;
        }
    }

    private static class SaslServerCallbackHandler implements CallbackHandler {

        /**
         * This implementation is take from ZookKeeper SASL/DIGEST-MD5 code
         */
        private static final String USER_PREFIX = "user_";

        private String userName;
        private final Map<String, String> credentials = new HashMap<String, String>();

        public SaslServerCallbackHandler(Configuration configuration, String serverSection) throws IOException {

            AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(serverSection);

            if (configurationEntries == null) {
                String errorMessage = "Could not find a '" + serverSection + "' entry in this configuration: Server cannot start.";

                throw new IOException(errorMessage);
            }
            credentials.clear();
            for (AppConfigurationEntry entry : configurationEntries) {
                Map<String, ?> options = entry.getOptions();
                // Populate DIGEST-MD5 user -> password map with JAAS configuration entries from the "Server" section.
                // Usernames are distinguished from other options by prefixing the username with a "user_" prefix.
                for (Map.Entry<String, ?> pair : options.entrySet()) {
                    String key = pair.getKey();
                    if (key.startsWith(USER_PREFIX)) {
                        String userName = key.substring(USER_PREFIX.length());
                        credentials.put(userName, (String) pair.getValue());
                    }
                }
            }
        }

        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    handleNameCallback((NameCallback) callback);
                } else if (callback instanceof PasswordCallback) {
                    handlePasswordCallback((PasswordCallback) callback);
                } else if (callback instanceof RealmCallback) {
                    handleRealmCallback((RealmCallback) callback);
                } else if (callback instanceof AuthorizeCallback) {
                    handleAuthorizeCallback((AuthorizeCallback) callback);
                }
            }
        }

        private void handleNameCallback(NameCallback nc) {
            // check to see if this user is in the user password database.
            if (credentials.get(nc.getDefaultName()) == null) {
                LOG.info("User '" + nc.getDefaultName() + "' not found in list of JAAS DIGEST-MD5 users.");
                return;
            }
            nc.setName(nc.getDefaultName());
            userName = nc.getDefaultName();
        }

        private void handlePasswordCallback(PasswordCallback pc) {
            if (credentials.containsKey(userName)) {
                pc.setPassword(credentials.get(userName).toCharArray());
            } else {
                LOG.info("No password found for user: " + userName);
            }
        }

        private void handleRealmCallback(RealmCallback rc) {
            LOG.info("client supplied realm: " + rc.getDefaultText());
            rc.setText(rc.getDefaultText());
        }

        private void handleAuthorizeCallback(AuthorizeCallback ac) {
            String authenticationID = ac.getAuthenticationID();
            String authorizationID = ac.getAuthorizationID();

            LOG.info("Successfully authenticated client: authenticationID=" + authenticationID
                + ";  authorizationID=" + authorizationID + ".");
            ac.setAuthorized(true);

            KerberosName kerberosName = new KerberosName(authenticationID);
            try {
                StringBuilder userNameBuilder = new StringBuilder(kerberosName.getShortName());
                userNameBuilder.append("/").append(kerberosName.getHostName());
                userNameBuilder.append("@").append(kerberosName.getRealm());
                ac.setAuthorizedID(userNameBuilder.toString());
            } catch (IOException e) {
                LOG.error("Failed to set name based on Kerberos authentication rules.", e);
            }
        }

    }
}
