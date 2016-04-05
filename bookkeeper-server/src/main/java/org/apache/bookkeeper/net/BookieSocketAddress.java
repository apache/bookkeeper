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
package org.apache.bookkeeper.net;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.apache.bookkeeper.util.BookKeeperConstants.COLON;
import org.jboss.netty.channel.local.LocalAddress;

/**
 * This is a data wrapper class that is an InetSocketAddress, it would use the hostname
 * provided in constructors directly.
 * <p>
 * The string representation of a BookieSocketAddress is : <hostname>:<port>
 */
public class BookieSocketAddress {

    // Member fields that make up this class.
    private final String hostname;
    private final int port;

    private final InetSocketAddress socketAddress;

    // Constructor that takes in both a port.
    public BookieSocketAddress(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
        socketAddress = new InetSocketAddress(hostname, port);
    }

    // Constructor from a String "serialized" version of this class.
    public BookieSocketAddress(String addr) throws UnknownHostException {
        String[] parts = addr.split(COLON);
        if (parts.length < 2) {
            throw new UnknownHostException(addr);
        }
        this.hostname = parts[0];
        try {
            this.port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException nfe) {
            throw new UnknownHostException(addr);
        }
        socketAddress = new InetSocketAddress(hostname, port);
    }

    // Public getters
    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    // Method to return an InetSocketAddress for the regular port.
    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    /**
     * Maps the socketAddress to a "local" address
     */
    public LocalAddress getLocalAddress() {
        return new LocalAddress(socketAddress.toString());
    }

    // Return the String "serialized" version of this object.
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(hostname).append(COLON).append(port);
        return sb.toString();
    }

    // Implement an equals method comparing two BookiSocketAddress objects.
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BookieSocketAddress))
            return false;
        BookieSocketAddress that = (BookieSocketAddress) obj;
        return this.hostname.equals(that.hostname) && (this.port == that.port);
    }

    @Override
    public int hashCode() {
        return this.hostname.hashCode() + 13 * this.port;
    }

}
