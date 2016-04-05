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
import java.net.InetSocketAddress;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AuthMessage;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ExtensionRegistry;



public class AuthProviderFactoryFactory {
    static Logger LOG = LoggerFactory.getLogger(AuthProviderFactoryFactory.class);

    public static BookieAuthProvider.Factory newBookieAuthProviderFactory(ServerConfiguration conf,
                                                                          ExtensionRegistry registry) throws IOException {
        String factoryClassName = conf.getBookieAuthProviderFactoryClass();

        if (factoryClassName == null || factoryClassName.length() == 0) {
            return new NullBookieAuthProviderFactory();
        }

        BookieAuthProvider.Factory factory = ReflectionUtils.newInstance(factoryClassName,
                                                                         BookieAuthProvider.Factory.class);
        factory.init(conf, registry);
        return factory;
    }

    public static ClientAuthProvider.Factory newClientAuthProviderFactory(ClientConfiguration conf,
                                                                          ExtensionRegistry registry) throws IOException {
        String factoryClassName = conf.getClientAuthProviderFactoryClass();

        if (factoryClassName == null || factoryClassName.length() == 0) {
            return new NullClientAuthProviderFactory();
        }

        ClientAuthProvider.Factory factory = ReflectionUtils.newInstance(factoryClassName,
                                                                         ClientAuthProvider.Factory.class);
        factory.init(conf, registry);
        return factory;
    }

    private final static String nullPluginName = "NULLPlugin";

    private static class NullBookieAuthProviderFactory implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return nullPluginName;
        }

        @Override
        public void init(ServerConfiguration conf, ExtensionRegistry registry) {}

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              GenericCallback<Void> completeCb) {
            completeCb.operationComplete(BKException.Code.OK, null);
            return new BookieAuthProvider() {
                public void process(AuthMessage m, GenericCallback<AuthMessage> cb) {}
            };
        }
    }

    private static class NullClientAuthProviderFactory implements ClientAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return nullPluginName;
        }

        @Override
        public void init(ClientConfiguration conf, ExtensionRegistry registry) {}

        @Override
        public ClientAuthProvider newProvider(InetSocketAddress addr,
                                              GenericCallback<Void> completeCb) {
            completeCb.operationComplete(BKException.Code.OK, null);
            return new ClientAuthProvider() {
                public void init(GenericCallback<AuthMessage> cb) {}
                public void process(AuthMessage m, GenericCallback<AuthMessage> cb) {}
            };
        }
    }

}
