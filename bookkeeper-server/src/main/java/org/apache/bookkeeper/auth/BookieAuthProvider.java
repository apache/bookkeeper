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
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieConnectionPeer;


/**
 * Bookie authentication provider interface.
 *
 * <p>This must be implemented by any party wishing to implement
 * an authentication mechanism for bookkeeper connections.
 */
public interface BookieAuthProvider {
    /**
     * A factory to create the bookie authentication provider.
     */
    interface Factory {
        /**
         * Initialize the factory with the server configuration
         * and protobuf message registry. Implementors must
         * add any extention messages which contain the auth
         * payload, so that the server can decode auth messages
         * it receives from the client.
         */
        void init(ServerConfiguration conf) throws IOException;

        /**
         * Create a new instance of a bookie auth provider.
         * Each connection should get its own instance, as they
         * can hold connection specific state.
         * The completeCb is used to notify the server that
         * the authentication handshake is complete.
         * CompleteCb should be called only once.
         * If the authentication was successful, BKException.Code.OK
         * should be passed as the return code. Otherwise, another
         * error code should be passed.
         * If authentication fails, the server will close the
         * connection.
         * @param connection an handle to the connection
         * @param completeCb callback to be notified when authentication
         *                   is complete.
         */
        BookieAuthProvider newProvider(BookieConnectionPeer connection,
                                       AuthCallbacks.GenericCallback<Void> completeCb);

        /**
         * Get Auth provider plugin name.
         * Used as a sanity check to ensure that the bookie and the client.
         * are using the same auth provider.
         */
        String getPluginName();

        /**
        * Release resources.
        */
        default void close() {}
    }

    /**
     * Callback to let the provider know that the underlying protocol is changed.
     * For instance this will happen when a START_TLS operation succeeds
     */
    default void onProtocolUpgrade() {
    }

    /**
     * Process a request from the client. cb will receive the next
     * message to be sent to the client. If there are no more messages
     * to send to the client, cb should not be called, and completeCb
     * must be called instead.
     */
    void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb);

    /**
     * Release resources.
     */
    default void close() {}
}
