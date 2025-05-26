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

import static org.apache.bookkeeper.clients.utils.NetUtils.createEndpoint;
import static org.apache.bookkeeper.clients.utils.NetUtils.parseEndpoint;
import static org.apache.bookkeeper.clients.utils.NetUtils.parseEndpoints;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

/**
 * Unit test for {@link NetUtils}.
 */
public class TestNetUtils {

    @Test
    public void testCreateEndpoint() {
        String hostname = "10.138.10.56";
        int port = 12345;
        Endpoint endpoint = createEndpoint(hostname, port);
        assertEquals(hostname, endpoint.getHostname());
        assertEquals(port, endpoint.getPort());
    }

    @Test
    public void testParseEndpoint() {
        String endpointStr = "10.138.10.56:12345";
        Endpoint endpoint = parseEndpoint(endpointStr);
        assertEquals("10.138.10.56", endpoint.getHostname());
        assertEquals(12345, endpoint.getPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseInvalidEndpoint1() {
        parseEndpoint("10.138.10.56");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseInvalidEndpoint2() {
        parseEndpoint("10.138.10.56:");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseInvalidEndpoint3() {
        parseEndpoint("10.138.10.56:123456:123456");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseInvalidEndpointPort() {
        parseEndpoint("10.138.10.56:abcd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseEmptyEndpoints() {
        parseEndpoint("");
    }

    @Test
    public void testParseEndpoints() {
        List<String> hostnames = Lists.newArrayList(
            "10.138.10.56",
            "10.138.10.57",
            "10.138.10.58");
        List<Integer> ports = Lists.newArrayList(
            1234,
            2345,
            3456);
        List<Endpoint> endpoints = IntStream.range(0, hostnames.size())
            .mapToObj(i -> createEndpoint(hostnames.get(i), ports.get(i)))
            .collect(Collectors.toList());
        String endpointStr =
            StringUtils.join(
                IntStream.range(0, hostnames.size())
                    .mapToObj(i -> hostnames.get(i) + ":" + ports.get(i))
                    .collect(Collectors.toList()),
                ',');
        List<Endpoint> parsedEndpoints = parseEndpoints(endpointStr);
        assertEquals(endpoints, parsedEndpoints);
    }

    @Test
    public void testToInetSocketAddress() {
        String hostname = "127.0.0.1";
        int port = 8080;
        Endpoint endpoint = createEndpoint(hostname, port);
        InetSocketAddress socketAddress = new InetSocketAddress(hostname, port);
        assertEquals(socketAddress, NetUtils.of(endpoint));
    }

}
