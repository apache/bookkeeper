/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients.utils;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.commons.lang.StringUtils;

/**
 * Network related utils.
 */
public class NetUtils {

    /**
     * Get the inet socket address for the provided {@code endpoint}.
     *
     * @param endpoint network endpoint
     * @return inet socket address for the network endpoint
     */
    public static InetSocketAddress of(Endpoint endpoint) {
        return new InetSocketAddress(endpoint.getHostname(), endpoint.getPort());
    }

    public static String getLocalHostName(boolean useHostname) {
        String hostname;
        try {
            if (useHostname) {
                hostname = InetAddress.getLocalHost().getHostName();
            } else {
                hostname = InetAddress.getLocalHost().getHostAddress();
            }
        } catch (UnknownHostException uhe) {
            hostname = "Unknown-Host-Name";
        }
        return hostname;
    }

    public static Endpoint getLocalEndpoint(int port, boolean useHostname)
        throws UnknownHostException {
        String hostname;
        if (useHostname) {
            hostname = InetAddress.getLocalHost().getHostName();
        } else {
            hostname = InetAddress.getLocalHost().getHostAddress();
        }
        return createEndpoint(hostname, port);
    }

    /**
     * Get the list of endpoints from an endpoint string.
     *
     * @param endpointStr an endpoint string.
     * @return list of endpoints.
     */
    public static List<Endpoint> parseEndpoints(String endpointStr) {
        String[] endpointParts = StringUtils.split(endpointStr, ',');
        checkArgument(endpointParts.length > 0,
            "Invalid endpoint strings %s", endpointStr);
        List<Endpoint> endpoints = Lists.newArrayListWithExpectedSize(endpointParts.length);
        for (String endpointPart : endpointParts) {
            endpoints.add(parseEndpoint(endpointPart));
        }
        return endpoints;
    }

    /**
     * Parse an endpoint from an endpoint string.
     *
     * @param endpointStr an endpoint string.
     * @return endpoint.
     */
    public static Endpoint parseEndpoint(String endpointStr) {
        String[] endpointParts = StringUtils.split(endpointStr, ':');
        checkArgument(2 == endpointParts.length,
            "Invalid endpoint string %s - It should be 'host:port'.", endpointStr);
        String host = endpointParts[0];
        int port;
        try {
            port = Integer.parseInt(endpointParts[1]);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Invalid port found in the endpoint string " + endpointStr, nfe);
        }
        return createEndpoint(host, port);
    }

    /**
     * Create an endpoint from {@code hostname} and {@code port}.
     *
     * @param hostname hostname of the endpoint
     * @param port     port of the endpoint
     * @return an endpoint created from {@code hostname} and {@code port}.
     */
    public static Endpoint createEndpoint(String hostname, int port) {
        return Endpoint.newBuilder()
            .setHostname(hostname)
            .setPort(port)
            .build();
    }

    /**
     * Convert an endpoint to string.
     *
     * @param ep endpoint
     * @return endpoint string representation
     */
    public static String endpointToString(Endpoint ep) {
        StringBuilder sb = new StringBuilder();
        sb.append(ep.getHostname()).append(":").append(ep.getPort());
        return sb.toString();
    }

}
