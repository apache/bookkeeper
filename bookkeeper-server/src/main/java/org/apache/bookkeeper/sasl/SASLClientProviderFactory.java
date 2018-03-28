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

import static org.apache.bookkeeper.conf.ClientConfiguration.CLIENT_ROLE_SYSTEM;
import static org.apache.bookkeeper.sasl.SaslConstants.JAAS_AUDITOR_SECTION_NAME;
import static org.apache.bookkeeper.sasl.SaslConstants.JAAS_CLIENT_SECTION_NAME;
import static org.apache.bookkeeper.sasl.SaslConstants.JAAS_DEFAULT_AUDITOR_SECTION_NAME;
import static org.apache.bookkeeper.sasl.SaslConstants.JAAS_DEFAULT_CLIENT_SECTION_NAME;

import java.io.IOException;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;

import org.apache.bookkeeper.auth.AuthCallbacks;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.ClientConnectionPeer;
import org.slf4j.LoggerFactory;

/**
 * ClientAuthProvider which uses JDK-bundled SASL.
 */
public class SASLClientProviderFactory implements
    org.apache.bookkeeper.auth.ClientAuthProvider.Factory, JAASCredentialsContainer {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SASLClientProviderFactory.class);

    private ClientConfiguration clientConfiguration;
    private LoginContext login;
    private Subject subject;
    private String principal;
    private boolean isKrbTicket;
    private boolean isUsingTicketCache;
    private String loginContextName;
    private TGTRefreshThread ticketRefreshThread;

    @Override
    public void init(ClientConfiguration conf) throws IOException {
        this.clientConfiguration = conf;
        try {

            this.login = loginClient();
            this.subject = login.getSubject();
            this.isKrbTicket = !subject.getPrivateCredentials(KerberosTicket.class).isEmpty();
            boolean systemRole = CLIENT_ROLE_SYSTEM.equals(clientConfiguration.getClientRole());
            this.loginContextName = systemRole
                ? clientConfiguration.getString(JAAS_AUDITOR_SECTION_NAME, JAAS_DEFAULT_AUDITOR_SECTION_NAME)
                : clientConfiguration.getString(JAAS_CLIENT_SECTION_NAME, JAAS_DEFAULT_CLIENT_SECTION_NAME);
            if (isKrbTicket) {
                this.isUsingTicketCache = SaslConstants.isUsingTicketCache(loginContextName);
                this.principal = SaslConstants.getPrincipal(loginContextName);
                ticketRefreshThread = new TGTRefreshThread(this);
                ticketRefreshThread.start();
            }
        } catch (SaslException | LoginException error) {
            throw new IOException(error);
        }
    }

    @Override
    public ClientAuthProvider newProvider(ClientConnectionPeer addr, AuthCallbacks.GenericCallback<Void> completeCb) {
        return new SASLClientAuthProvider(addr, completeCb, subject);
    }

    @Override
    public String getPluginName() {
        return SaslConstants.PLUGIN_NAME;
    }

    private LoginContext loginClient() throws SaslException, LoginException {
        boolean systemRole = ClientConfiguration.CLIENT_ROLE_SYSTEM.equals(clientConfiguration.getClientRole());
        String configurationEntry = systemRole
            ? clientConfiguration.getString(JAAS_AUDITOR_SECTION_NAME, JAAS_DEFAULT_AUDITOR_SECTION_NAME)
            : clientConfiguration.getString(JAAS_CLIENT_SECTION_NAME, JAAS_DEFAULT_CLIENT_SECTION_NAME);
        AppConfigurationEntry[] entries = Configuration.getConfiguration()
            .getAppConfigurationEntry(configurationEntry);
        if (entries == null) {
            LOG.info("No JAAS Configuration found with section BookKeeper");
            return null;
        }
        try {
            LoginContext loginContext = new LoginContext(configurationEntry,
                    new SaslClientState.ClientCallbackHandler(null));
            loginContext.login();
            return loginContext;
        } catch (LoginException error) {
            LOG.error("Error JAAS Configuration subject", error);
            return null;
        }
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
    public LoginContext getLogin() {
        return login;
    }

    @Override
    public void setLogin(LoginContext login) {
        this.login = login;
    }

    @Override
    public Subject getSubject() {
        return subject;
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
        return clientConfiguration;
    }

    @Override
    public String getLoginContextName() {
        return loginContextName;
    }

}
