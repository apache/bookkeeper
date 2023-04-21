/*
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

package org.apache.bookkeeper.common.reslover;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Lists;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.bookkeeper.common.resolver.StaticNameResolver;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.junit.Test;

/**
 * Unit test of {@link StaticNameResolver}.
 */
public class TestSimpleNameResolver {

    static List<InetSocketAddress> createServers(int numBookies) {
        List<InetSocketAddress> servers = Lists.newArrayListWithExpectedSize(numBookies);
        int basePort = 3181;
        for (int i = 0; i < numBookies; i++) {
            servers.add(new InetSocketAddress("127.0.0.1", basePort + i));
        }
        return servers;
    }

    @Test
    public void testGetServers() {
        List<InetSocketAddress> servers = createServers(5);
        List<URI> uris = servers
            .stream()
            .map(addr -> URI.create("bookie://" + addr.getHostName() + ":" + addr.getPort()))
            .collect(Collectors.toList());
        List<EquivalentAddressGroup> resolvedServers = servers
            .stream()
            .map(addr -> new EquivalentAddressGroup(
                Lists.newArrayList(addr),
                Attributes.EMPTY))
            .collect(Collectors.toList());

        @SuppressWarnings("unchecked") // for the mock
            StaticNameResolver nameResolver = new StaticNameResolver(
            "test-name-resolver",
            mock(Resource.class),
            uris);

        assertEquals(resolvedServers, nameResolver.getServers());
    }

}
