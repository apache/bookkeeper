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
import java.util.regex.PatternSyntaxException;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.SaslException;

import org.apache.bookkeeper.auth.AuthCallbacks;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieConnectionPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BookieAuthProvider which uses JDK-bundled SASL.
 */
public class SASLBookieAuthProviderFactory implements org.apache.bookkeeper.auth.BookieAuthProvider.Factory,
    JAASCredentialsContainer {

    private static final Logger LOG = LoggerFactory.getLogger(SASLBookieAuthProviderFactory.class);

    private Pattern allowedIdsPattern;
    private ServerConfiguration serverConfiguration;
    private Subject subject;
    private boolean isKrbTicket;
    private boolean isUsingTicketCache;
    private String principal;
    private String loginContextName;
    private LoginContext login;
    private TGTRefreshThread ticketRefreshThread;

    @Override
    public void init(ServerConfiguration conf) throws IOException {
        this.serverConfiguration = conf;

        final String allowedIdsPatternRegExp = conf.getString(SaslConstants.JAAS_CLIENT_ALLOWED_IDS,
            SaslConstants.JAAS_CLIENT_ALLOWED_IDS_DEFAULT);
        try {
            this.allowedIdsPattern = Pattern.compile(allowedIdsPatternRegExp);
        } catch (PatternSyntaxException error) {
            LOG.error("Invalid regular expression " + allowedIdsPatternRegExp, error);
            throw new IOException(error);
        }

        try {
            loginContextName = serverConfiguration.getString(SaslConstants.JAAS_BOOKIE_SECTION_NAME,
                SaslConstants.JAAS_DEFAULT_BOOKIE_SECTION_NAME);

            this.login = loginServer();
            this.subject = login.getSubject();
            this.isKrbTicket = !subject.getPrivateCredentials(KerberosTicket.class).isEmpty();
            if (isKrbTicket) {
                this.isUsingTicketCache = SaslConstants.isUsingTicketCache(loginContextName);
                this.principal = SaslConstants.getPrincipal(loginContextName);
                this.ticketRefreshThread = new TGTRefreshThread(this);
                ticketRefreshThread.start();
            }
        } catch (SaslException | LoginException error) {
            throw new IOException(error);
        }
    }

    @Override
    public org.apache.bookkeeper.auth.BookieAuthProvider newProvider(BookieConnectionPeer addr,
        AuthCallbacks.GenericCallback<Void> completeCb) {
        return new SASLBookieAuthProvider(addr, completeCb, serverConfiguration,
            subject, allowedIdsPattern);
    }

    @Override
    public String getPluginName() {
        return SaslConstants.PLUGIN_NAME;
    }

    @Override
    public void close() {
        if (ticketRefreshThread != null) {
            ticketRefreshThread.interrupt();
            try {
                ticketRefreshThread.join(10000);
            } catch (InterruptedException exit) {
                Thread.currentThread().interrupt();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("interrupted while waiting for TGT reresh thread to stop", exit);
                }
            }
        }
    }

    @Override
    public Subject getSubject() {
        return subject;
    }

    @Override
    public LoginContext getLogin() {
        return login;
    }

    @Override
    public void setLogin(LoginContext login) {
        this.login = login;
    }

    @Override
    public boolean isUsingTicketCache() {
        return isUsingTicketCache;
    }

    @Override
    public String getPrincipal() {
        return principal;
    }

    @Override
    public AbstractConfiguration getConfiguration() {
        return serverConfiguration;
    }

    @Override
    public String getLoginContextName() {
        return loginContextName;
    }

    private LoginContext loginServer() throws SaslException, LoginException {

        AppConfigurationEntry[] entries = Configuration.getConfiguration()
            .getAppConfigurationEntry(loginContextName);
        if (entries == null) {
            LOG.info("JAAS not configured or no "
                + loginContextName + " present in JAAS Configuration file");
            return null;
        }
        LoginContext loginContext = new LoginContext(loginContextName, new ClientCallbackHandler(null));
        loginContext.login();
        return loginContext;

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
}
