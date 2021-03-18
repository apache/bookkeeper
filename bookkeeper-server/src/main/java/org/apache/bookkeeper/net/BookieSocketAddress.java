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

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import org.apache.bookkeeper.proto.BookieAddressResolver;

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

    /**
     * Create a BookieID in legacy format hostname:port.
     * @return the BookieID
     */
    public BookieId toBookieId() {
        return BookieId.parse(this.toString());
    }

    /**
     * Simple converter from legacy BookieId to a real network address.
     */
    public static final BookieAddressResolver LEGACY_BOOKIEID_RESOLVER = (BookieId b) -> {
        try {
            return new BookieSocketAddress(b.toString());
        } catch (UnknownHostException err) {
            throw new BookieAddressResolver.BookieIdNotResolvedException(b, err);
        }
    };

    /**
     * Utility for Placement Policies that need to create a dummy BookieId that represents
     * a given host.
     * @param hostname the hostname
     * @return a dummy bookie id, compatible with the BookieSocketAddress#toBookieId, with a 0 tcp port.
     */
    public static BookieId createDummyBookieIdForHostname(String hostname) {
        return BookieId.parse(hostname + ":0");
    }

    /**
     * Tells whether a BookieId may be a dummy id.
     * @param bookieId
     * @return true if the BookieId looks like it has been generated by
     * {@link #createDummyBookieIdForHostname(java.lang.String)}
     */
    public static boolean isDummyBookieIdForHostname(BookieId bookieId) {
        return bookieId.getId().endsWith(":0");
    }

    /**
     * Use legacy resolver to resolve a bookieId.
     * @param bookieId id supposed to be generated by
     * {@link #createDummyBookieIdForHostname(java.lang.String)}
     * @return the BookieSocketAddress
     */
    public static BookieSocketAddress resolveDummyBookieId(BookieId bookieId)
            throws BookieAddressResolver.BookieIdNotResolvedException {
        return LEGACY_BOOKIEID_RESOLVER.resolve(bookieId);
    }

}
