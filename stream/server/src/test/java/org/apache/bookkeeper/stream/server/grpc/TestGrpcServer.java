/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.grpc.util.MutableHandlerRegistry;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stream.server.StorageServer;
import org.apache.bookkeeper.stream.server.conf.StorageServerConfiguration;
import org.apache.bookkeeper.stream.storage.impl.StorageContainerStoreImpl;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test of {@link GrpcServer}.
 */
public class TestGrpcServer {

    @Rule
    public TestName name = new TestName();

    private final CompositeConfiguration compConf = new CompositeConfiguration();

    @Test
    public void testCreateLocalServer() {
        GrpcServer server = new GrpcServer(
            mock(StorageContainerStoreImpl.class),
            StorageServerConfiguration.of(compConf),
            null,
            name.getMethodName(),
            new MutableHandlerRegistry(),
            NullStatsLogger.INSTANCE);
        server.start();
        assertEquals(-1, server.getGrpcServer().getPort());
        server.close();
    }

    @Test
    public void testCreateBindServer() throws Exception {
        GrpcServer server = new GrpcServer(
            mock(StorageContainerStoreImpl.class),
            StorageServerConfiguration.of(compConf),
            StorageServer.createLocalEndpoint(0, false),
            null,
            null,
            NullStatsLogger.INSTANCE);
        server.start();
        assertTrue(server.getGrpcServer().getPort() > 0);
        server.close();
    }

}
