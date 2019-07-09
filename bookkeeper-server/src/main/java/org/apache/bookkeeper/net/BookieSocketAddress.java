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

import static org.apache.bookkeeper.util.BookKeeperConstants.COLON;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.net.InetAddresses;
import io.netty.channel.local.LocalAddress;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;

/**
 * This is a data wrapper class that is an InetSocketAddress, it would use the hostname
 * provided in constructors directly.
 *
 * <p>The string representation of a BookieSocketAddress is : &lt;hostname&gt;:&lt;port&gt;
 */
public class BookieSocketAddress {

    // Member fields that make up this class.
    private final String hostname;
    private final int port;
    private final Optional<InetSocketAddress> socketAddress;

    // Constructor that takes in both a port.
    public BookieSocketAddress(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
        /*
         * if ipaddress is used for bookieid then lets cache InetSocketAddress
         * otherwise not cache it. If Hostname is used for bookieid, then it is
         * ok for node to change its ipaddress. But if ipaddress is used for
         * bookieid then it is invalid scenario if node's ipaddress changes and
         * nodes HostName is considered static.
         */
        if (InetAddresses.isInetAddress(hostname)) {
            socketAddress = Optional.of(new InetSocketAddress(hostname, port));
        } else {
            socketAddress = Optional.empty();
        }
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
        if (InetAddresses.isInetAddress(hostname)) {
            socketAddress = Optional.of(new InetSocketAddress(hostname, port));
        } else {
            socketAddress = Optional.empty();
        }
    }



    // Public getters
    public String getHostName() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    // Method to return an InetSocketAddress for the regular port.
    @JsonIgnore
    public InetSocketAddress getSocketAddress() {
        /*
         * Return each time a new instance of the InetSocketAddress if hostname
         * is used as bookieid. If we keep using the same InetSocketAddress
         * instance, if bookies are advertising hostnames and the IP change, the
         * BK client will keep forever to try to connect to the old IP.
         */
        return socketAddress.orElseGet(() -> {
            return new InetSocketAddress(hostname, port);
        });
    }

    /**
     * Maps the socketAddress to a "local" address.
     */
    @JsonIgnore
    public LocalAddress getLocalAddress() {
        // for local address, we just need "port" to differentiate different addresses.
        return new LocalAddress("" + port);
    }

    // Return the String "serialized" version of this object.
    @Override
    public String toString() {
        return hostname + COLON + port;
    }

    // Implement an equals method comparing two BookiSocketAddress objects.
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BookieSocketAddress)) {
            return false;
        }
        BookieSocketAddress that = (BookieSocketAddress) obj;
        return this.hostname.equals(that.hostname) && (this.port == that.port);
    }

    @Override
    public int hashCode() {
        return this.hostname.hashCode() + 13 * this.port;
    }

}
