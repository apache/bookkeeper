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

package org.apache.bookkeeper.client.resolver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.common.collect.Lists;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolver.Listener;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Test;

/**
 * Unit test of {@link SimpleNameResolverFactory}.
 */
public class TestSimpleNameResolverFactory {

    static List<BookieSocketAddress> createServers(int numBookies) {
        List<BookieSocketAddress> servers = Lists.newArrayListWithExpectedSize(numBookies);
        int basePort = 3181;
        for (int i = 0; i < numBookies; i++) {
            servers.add(new BookieSocketAddress("127.0.0.1", basePort + i));
        }
        return servers;
    }

    private final List<BookieSocketAddress> servers;
    private final SimpleNameResolverFactory factory;

    public TestSimpleNameResolverFactory() {
        servers = createServers(5);
        factory = SimpleNameResolverFactory.of(servers);
    }

    @Test
    public void testGetName() {
        assertEquals("simple", factory.name());
    }

    @Test
    public void testGetDefaultScheme() {
        assertEquals("bookie", factory.getDefaultScheme());
    }

    @Test
    public void testNewNameResolverWithWrongTargetURI() {
        URI uri = URI.create("wrong://path/to/name");
        assertNull(factory.newNameResolver(uri, Attributes.EMPTY));
    }

    @Test
    public void testNewSimpleNameResolver() throws Exception {
        URI uri = URI.create("bookie:///bootstrap");
        NameResolver resolver = factory.newNameResolver(uri, Attributes.EMPTY);
        assertNotNull(resolver);
        List<EquivalentAddressGroup> resolvedServers = servers.stream()
            .map(addr -> new EquivalentAddressGroup(
                Lists.newArrayList(addr.getSocketAddress()),
                Attributes.EMPTY))
            .collect(Collectors.toList());

        CompletableFuture<List<EquivalentAddressGroup>> startFuture = FutureUtils.createFuture();
        resolver.start(new Listener() {
            @SuppressWarnings("deprecation")
            @Override
            public void onUpdate(List<io.grpc.ResolvedServerInfoGroup> servers,
                                 Attributes attributes) {
                // no-op
            }

            @Override
            public void onAddresses(List<EquivalentAddressGroup> servers, Attributes attributes) {
                startFuture.complete(servers);
            }

            @Override
            public void onError(Status error) {
                startFuture.completeExceptionally(new StatusRuntimeException(error));
            }
        });

        assertEquals(resolvedServers, startFuture.get());
    }

}
