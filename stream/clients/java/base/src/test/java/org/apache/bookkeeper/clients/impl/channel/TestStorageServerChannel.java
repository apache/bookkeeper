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

package org.apache.bookkeeper.clients.impl.channel;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.util.MutableHandlerRegistry;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link StorageServerChannel}.
 */
public class TestStorageServerChannel {

    private final String serverName = "fake server for " + getClass();
    private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
    private Server fakeServer;

    @Before
    public void setUp() throws Exception {
        fakeServer = InProcessServerBuilder
            .forName(serverName)
            .fallbackHandlerRegistry(serviceRegistry)
            .directExecutor()
            .build()
            .start();
    }

    @After
    public void tearDown() throws Exception {
        if (null != fakeServer) {
            fakeServer.shutdown();
        }
    }

    @Test
    public void testBasic() {
        ManagedChannel managedChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        Optional<String> token = Optional.empty();
        StorageServerChannel channel = new StorageServerChannel(managedChannel, token);
        assertNotNull(channel.getRootRangeService());
        assertNotNull(channel.getMetaRangeService());
        assertNotNull(channel.getStorageContainerService());
        assertNotNull(channel.getTableService());
        channel.close();
    }

    @Test
    public void testIntercept() {
        ManagedChannel channel = mock(ManagedChannel.class);
        StorageServerChannel ssChannel = new StorageServerChannel(channel, Optional.empty());
        StorageServerChannel interceptedChannel = ssChannel.intercept(1L);
        interceptedChannel.close();
        verify(channel, times(1)).shutdown();
    }

}
