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
package org.apache.bookkeeper.auth;

import java.io.IOException;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.bookkeeper.client.ClientConnectionPeer;
import org.apache.bookkeeper.bookie.BookieConnectionPeer;


public class AuthProviderFactoryFactory {
    static Logger LOG = LoggerFactory.getLogger(AuthProviderFactoryFactory.class);

    public static BookieAuthProvider.Factory newBookieAuthProviderFactory(ServerConfiguration conf) throws IOException {
        String factoryClassName = conf.getBookieAuthProviderFactoryClass();

        if (factoryClassName == null || factoryClassName.length() == 0) {
            return new AuthenticationDisabledAuthProviderFactory();
        }

        BookieAuthProvider.Factory factory = ReflectionUtils.newInstance(factoryClassName,
                                                                         BookieAuthProvider.Factory.class);
        factory.init(conf);
        return factory;
    }

    public static ClientAuthProvider.Factory newClientAuthProviderFactory(ClientConfiguration conf) throws IOException {
        String factoryClassName = conf.getClientAuthProviderFactoryClass();

        if (factoryClassName == null || factoryClassName.length() == 0) {
            return new NullClientAuthProviderFactory();
        }

        ClientAuthProvider.Factory factory = ReflectionUtils.newInstance(factoryClassName,
                                                                         ClientAuthProvider.Factory.class);
        factory.init(conf);
        return factory;
    }

    public final static String authenticationDisabledPluginName = "AuthDisabledPlugin";

    private static class AuthenticationDisabledAuthProviderFactory implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return authenticationDisabledPluginName;
        }

        @Override
        public void init(ServerConfiguration conf) {}

        @Override
        public BookieAuthProvider newProvider(BookieConnectionPeer addr,
                                              AuthCallbacks.GenericCallback<Void> completeCb) {
            completeCb.operationComplete(BKException.Code.OK, null);
            return new BookieAuthProvider() {
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                    // any request of authentication for clients is going to be answered with a standard response
                    // the client will d
                    addr.setAuthorizedId(BookKeeperPrincipal.ANONYMOUS);
                    cb.operationComplete(BKException.Code.OK, AuthToken.NULL);
                }
            };
        }
    }

    private static class NullClientAuthProviderFactory implements ClientAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return authenticationDisabledPluginName;
        }

        @Override
        public void init(ClientConfiguration conf) {}

        @Override
        public ClientAuthProvider newProvider(ClientConnectionPeer addr,
                                              AuthCallbacks.GenericCallback<Void> completeCb) {
            addr.setAuthorizedId(BookKeeperPrincipal.ANONYMOUS);
            completeCb.operationComplete(BKException.Code.OK, null);
            return new ClientAuthProvider() {
                public void init(AuthCallbacks.GenericCallback<AuthToken> cb) {}
                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {}
            };
        }
    }

}
