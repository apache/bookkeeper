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

package org.apache.distributedlog.stream.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.grpc.util.MutableHandlerRegistry;
import java.util.Optional;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.distributedlog.stream.server.StorageServer;
import org.apache.distributedlog.stream.server.conf.StorageServerConfiguration;
import org.apache.distributedlog.stream.storage.impl.RangeStoreImpl;
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
      mock(RangeStoreImpl.class),
      StorageServerConfiguration.of(compConf),
      Optional.empty(),
      Optional.of(name.getMethodName()),
      Optional.of(new MutableHandlerRegistry()),
      NullStatsLogger.INSTANCE);
    server.start();
    assertEquals(-1, server.getGrpcServer().getPort());
    server.close();
  }

  @Test
  public void testCreateBindServer() throws Exception {
    GrpcServer server = new GrpcServer(
      mock(RangeStoreImpl.class),
      StorageServerConfiguration.of(compConf),
      Optional.of(StorageServer.createLocalEndpoint(0, false)),
      Optional.empty(),
      Optional.empty(),
      NullStatsLogger.INSTANCE);
    server.start();
    assertTrue(server.getGrpcServer().getPort() > 0);
    server.close();
  }

}
