/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.net;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Network Utilities.
 */
public class NetUtils {
    private static final Logger logger = LoggerFactory.getLogger(NetUtils.class);

    /**
     * Given a string representation of a host, return its ip address
     * in textual presentation.
     *
     * @param name a string representation of a host:
     *             either a textual representation its IP address or its host name
     * @return its IP address in the string format
     */
    public static String normalizeHostName(String name) {
        try {
            return InetAddress.getByName(name).getHostAddress();
        } catch (UnknownHostException e) {
            return name;
        }
    }

    /**
     * Given a collection of string representation of hosts, return a list of
     * corresponding IP addresses in the textual representation.
     *
     * @param names a collection of string representations of hosts
     * @return a list of corresponding IP addresses in the string format
     * @see #normalizeHostName(String)
     */
    public static List<String> normalizeHostNames(Collection<String> names) {
        List<String> hostNames = new ArrayList<String>(names.size());
        for (String name : names) {
            hostNames.add(normalizeHostName(name));
        }
        return hostNames;
    }

    public static String resolveNetworkLocation(DNSToSwitchMapping dnsResolver,
                                                BookieSocketAddress addr) {
        List<String> names = new ArrayList<String>(1);

        InetSocketAddress inetSocketAddress = addr.getSocketAddress();
        if (dnsResolver.useHostName()) {
            names.add(addr.getHostName());
        } else {
            InetAddress inetAddress = inetSocketAddress.getAddress();
            if (null == inetAddress) {
                names.add(addr.getHostName());
            } else {
                names.add(inetAddress.getHostAddress());
            }
        }

        // resolve network addresses
        List<String> rNames = dnsResolver.resolve(names);
        checkNotNull(rNames, "DNS Resolver should not return null response.");
        checkState(rNames.size() == 1, "Expected exactly one element");

        return rNames.get(0);
    }

}
