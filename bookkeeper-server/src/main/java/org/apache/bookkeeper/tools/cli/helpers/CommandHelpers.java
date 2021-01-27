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
import java.net.InetAddress;
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

    private  static final String UNKNOWN = "UNKNOWN";
    /*
     * The string returned is of the form:
     * BookieID:bookieId, IP:ip, Port: port, Hostname: hostname
     * When using hostname as bookie id, it's possible that the host is no longer valid and
     * can't get a ip from the hostname, so using UNKNOWN to indicate ip is unknown for the hostname
     */
    public static String getBookieSocketAddrStringRepresentation(BookieId bookieId,
                                                                 BookieAddressResolver bookieAddressResolver) {
        try {
            BookieSocketAddress networkAddress = bookieAddressResolver.resolve(bookieId);
            String hostname = networkAddress.getHostName();
            String realHostname;
            String ip;
            if (InetAddresses.isInetAddress(hostname)){
                ip = hostname;
                realHostname = networkAddress.getSocketAddress().getAddress().getCanonicalHostName();
            } else {
               InetAddress ia = networkAddress.getSocketAddress().getAddress();
               if (null != ia){
                  ip = ia.getHostAddress();
               } else {
                  ip = UNKNOWN;
               }
               realHostname = hostname;
            }
            return formatBookieSocketAddress(bookieId, ip, networkAddress.getPort(), realHostname);
        } catch (BookieAddressResolver.BookieIdNotResolvedException bookieNotAvailable) {
            return formatBookieSocketAddress(bookieId, UNKNOWN, 0, UNKNOWN);
        }
    }

    /**
     * Format {@link BookieSocketAddress}.
     **/
    private static String formatBookieSocketAddress(BookieId bookieId, String ip, int port, String hostName) {
       return String.format("BookieID:%s, IP:%s, Port:%d, Hostname:%s", bookieId.toString(), ip, port, hostName);
    }

}
