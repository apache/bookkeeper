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
package org.apache.bookkeeper.tls;

import com.google.common.base.Strings;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.auth.AuthCallbacks;
import org.apache.bookkeeper.auth.AuthToken;
import org.apache.bookkeeper.auth.BookKeeperPrincipal;
import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieConnectionPeer;
import org.apache.bookkeeper.util.CertUtils;


/**
 * Authorization factory class.
 */
@Slf4j
public class BookieAuthZFactory implements BookieAuthProvider.Factory {

    public String[] allowedRoles;

    @Override
    public String getPluginName() {
        return "BookieAuthZFactory";
    }

    @Override
    public void init(ServerConfiguration conf) throws IOException {
        // Read from config
        allowedRoles = conf.getAuthorizedRoles();

        if (allowedRoles == null || allowedRoles.length == 0) {
            throw new RuntimeException("Configuration option \'bookieAuthProviderFactoryClass\' is set to"
                    + " \'BookieAuthZFactory\' but no roles set for configuration field \'authorizedRoles\'.");
        }

        // If authorization is enabled and there are no roles, exit
        for (String allowedRole : allowedRoles) {
            if (Strings.isNullOrEmpty(allowedRole)) {
                throw new RuntimeException("Configuration option \'bookieAuthProviderFactoryClass\' is set to"
                        + " \'BookieAuthZFactory\' but no roles set for configuration field \'authorizedRoles\'.");
            }
        }
    }

    @Override
    public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                          final AuthCallbacks.GenericCallback<Void> completeCb) {
        return new BookieAuthProvider() {

            AuthCallbacks.GenericCallback<Void> completeCallback = completeCb;

            @Override
            public void onProtocolUpgrade() {

                try {
                    boolean secureBookieSideChannel = addr.isSecure();
                    Collection<Object> certificates = addr.getProtocolPrincipals();
                    if (secureBookieSideChannel && !certificates.isEmpty()
                            && certificates.iterator().next() instanceof X509Certificate) {
                        X509Certificate tempCert = (X509Certificate) certificates.iterator().next();
                        String[] certRole = CertUtils.getRolesFromOU(tempCert);
                        if (certRole == null || certRole.length == 0) {
                            log.error("AuthZ failed: No cert role in OU field of certificate. Must have a role from "
                                            + "allowedRoles list {} host: {}",
                                    allowedRoles, addr.getRemoteAddr());
                            completeCallback.operationComplete(BKException.Code.UnauthorizedAccessException, null);
                            return;
                        }
                        boolean authorized = false;
                        for (String allowedRole : allowedRoles) {
                            if (certRole[0].equals(allowedRole)) {
                                authorized = true;
                                break;
                            }
                        }
                        if (authorized) {
                            addr.setAuthorizedId(new BookKeeperPrincipal(certRole[0]));
                            completeCallback.operationComplete(BKException.Code.OK, null);
                        } else {
                            log.error("AuthZ failed: Cert role {} doesn't match allowedRoles list {}; host: {}",
                                    certRole, allowedRoles, addr.getRemoteAddr());
                            completeCallback.operationComplete(BKException.Code.UnauthorizedAccessException, null);
                        }
                    } else {
                        if (!secureBookieSideChannel) {
                            log.error("AuthZ failed: Bookie side channel is not secured; host: {}",
                                    addr.getRemoteAddr());
                        } else if (certificates.isEmpty()) {
                            log.error("AuthZ failed: Certificate missing; host: {}", addr.getRemoteAddr());
                        } else {
                            log.error("AuthZ failed: Certs are missing or not X509 type; host: {}",
                                    addr.getRemoteAddr());
                        }
                        completeCallback.operationComplete(BKException.Code.UnauthorizedAccessException, null);
                    }
                } catch (Exception e) {
                    log.error("AuthZ failed: Failed to parse certificate; host: {}, {}", addr.getRemoteAddr(), e);
                    completeCallback.operationComplete(BKException.Code.UnauthorizedAccessException, null);
                }
            }

            @Override
            public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
            }
        };
    }


}
