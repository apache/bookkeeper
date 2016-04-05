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

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AuthMessage;

import com.google.protobuf.ExtensionRegistry;

/**
 * Client authentication provider interface.
 * This must be implemented by any party wishing to implement
 * an authentication mechanism for bookkeeper connections.
 */
public interface ClientAuthProvider {
    interface Factory {
        /**
         * Initialize the factory with the client configuration
         * and protobuf message registry. Implementors must
         * add any extention messages which contain the auth
         * payload, so that the client can decode auth messages
         * it receives from the server.
         */
        void init(ClientConfiguration conf,
                  ExtensionRegistry registry) throws IOException;

        /**
         * Create a new instance of a client auth provider.
         * Each connection should get its own instance, as they
         * can hold connection specific state.
         * The completeCb is used to notify the client that
         * the authentication handshake is complete.
         * CompleteCb should be called only once.
         * If the authentication was successful, BKException.Code.OK
         * should be passed as the return code. Otherwise, another
         * error code should be passed.
         * @param addr the address of the socket being authenticated
         * @param completeCb callback to be notified when authentication
         *                   is complete.
         */
        ClientAuthProvider newProvider(InetSocketAddress addr,
                                       GenericCallback<Void> completeCb);

        /**
         * Get Auth provider plugin name.
         * Used as a sanity check to ensure that the bookie and the client.
         * are using the same auth provider.
         */
        String getPluginName();
    }

    /**
     * Initiate the authentication. cb will receive the initial
     * authentication message which should be sent to the server.
     * cb may not be called if authentication is not requires. In
     * this case, completeCb should be called.
     */
    void init(GenericCallback<AuthMessage> cb);

    /**
     * Process a response from the server. cb will receive the next
     * message to be sent to the server. If there are no more messages
     * to send to the server, cb should not be called, and completeCb
     * must be called instead.
     */
    void process(AuthMessage m, GenericCallback<AuthMessage> cb);
}
