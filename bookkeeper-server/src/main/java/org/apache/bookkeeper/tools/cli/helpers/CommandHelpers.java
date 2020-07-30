/*
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
 */
package org.apache.bookkeeper.tools.cli.helpers;

import com.google.common.net.InetAddresses;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieAddressResolver;

/**
 * Helper classes used by the cli commands.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CommandHelpers {

    /*
     * The string returned is of the form:
     * 'hostname'('otherformofhostname'):'port number'
     *
     * where hostname and otherformofhostname are ipaddress and
     * canonicalhostname or viceversa
     */
    public static String getBookieSocketAddrStringRepresentation(BookieId bookidId, BookieAddressResolver bookieAddressResolver) {
        BookieSocketAddress networkAddress = bookieAddressResolver.resolve(bookidId);
        String hostname = networkAddress.getHostName();
        boolean isHostNameIpAddress = InetAddresses.isInetAddress(hostname);
        String otherFormOfHostname = null;
        if (isHostNameIpAddress) {
            otherFormOfHostname = networkAddress.getSocketAddress().getAddress().getCanonicalHostName();
        } else {
            otherFormOfHostname = networkAddress.getSocketAddress().getAddress().getHostAddress();
        }

        String bookieSocketAddrStringRepresentation;
        if (networkAddress.toBookieId().equals(bookidId)) {
            bookieSocketAddrStringRepresentation = hostname + "(" + otherFormOfHostname + ")" + ":"
                + networkAddress.getSocketAddress().getPort();
        } else {
            bookieSocketAddrStringRepresentation = bookidId+" - "+hostname + "(" + otherFormOfHostname + ")" + ":"
                + networkAddress.getSocketAddress().getPort();
        }
        return bookieSocketAddrStringRepresentation;
    }

}
