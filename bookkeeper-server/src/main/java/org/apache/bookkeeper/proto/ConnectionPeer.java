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
package org.apache.bookkeeper.proto;

import java.net.SocketAddress;
import java.util.Collection;
import org.apache.bookkeeper.auth.BookKeeperPrincipal;

/**
 * Represents the connection to a BookKeeper client, from the Bookie side.
 */
public interface ConnectionPeer {

    /**
     * Address from which originated the connection.
     * @return
     */
    SocketAddress getRemoteAddr();

    /**
     * Additional principals bound to the connection, like TLS certificates.
     * @return
     */
    Collection<Object> getProtocolPrincipals();

    /**
     * Utility function to be used from AuthProviders to drop the connection.
     */
    void disconnect();

    /**
     * Returns the user which is bound to the connection.
     * @return the principal or null if no auth takes place
     * or the auth plugin did not call {@link #setAuthorizedId(org.apache.bookkeeper.auth.BookKeeperPrincipal)}
     * @see  #setAuthorizedId(org.apache.bookkeeper.auth.BookKeeperPrincipal)
     */
    BookKeeperPrincipal getAuthorizedId();

    /**
     * Assign a principal to the current connection.
     * @param principal the id of the user
     * @see #getAuthorizedId()
     */
    void setAuthorizedId(BookKeeperPrincipal principal);

    /**
     * This flag returns true if a 'secure' channel in use, like TLS.
     * @return true if the channel is 'secure'
     */
    boolean isSecure();
}
