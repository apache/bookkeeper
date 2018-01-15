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

import com.google.common.annotations.VisibleForTesting;
import io.grpc.HandlerRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.stream.proto.common.Endpoint;
import org.apache.distributedlog.stream.server.conf.StorageServerConfiguration;
import org.apache.distributedlog.stream.server.exceptions.StorageServerRuntimeException;
import org.apache.distributedlog.stream.storage.api.RangeStore;

/**
 * KeyRange Server.
 */
@Slf4j
public class GrpcServer extends AbstractLifecycleComponent<StorageServerConfiguration> {

  public static GrpcServer build(GrpcServerSpec spec) {
    return new GrpcServer(
      spec.storeSupplier().get(),
      spec.storeServerConf(),
      spec.endpoint(),
      spec.localServerName(),
      spec.localHandlerRegistry(),
      spec.statsLogger());
  }

  private final Endpoint myEndpoint;
  private final Server grpcServer;

  public GrpcServer(RangeStore rangeStore,
                    StorageServerConfiguration conf,
                    Endpoint myEndpoint,
                    StatsLogger statsLogger) {
    this(rangeStore, conf, Optional.of(myEndpoint), Optional.empty(), Optional.empty(), statsLogger);
  }

  @VisibleForTesting
  public GrpcServer(RangeStore rangeStore,
                    StorageServerConfiguration conf,
                    Optional<Endpoint> myEndpoint,
                    Optional<String> localServerName,
                    Optional<HandlerRegistry> localHandlerRegistry,
                    StatsLogger statsLogger) {
    super("range-grpc-server", conf, statsLogger);
    if (myEndpoint.isPresent()) {
      this.myEndpoint = myEndpoint.get();
    } else {
      this.myEndpoint = null;
    }
    if (localServerName.isPresent()) {
      InProcessServerBuilder serverBuilder = InProcessServerBuilder
        .forName(localServerName.get())
        .directExecutor();
      if (localHandlerRegistry.isPresent()) {
        serverBuilder = serverBuilder.fallbackHandlerRegistry(localHandlerRegistry.get());
      }
      this.grpcServer = serverBuilder.build();
    } else {
      this.grpcServer = ServerBuilder
        .forPort(this.myEndpoint.getPort())
        .addService(new GrpcRootRangeService(rangeStore))
        .addService(new GrpcStorageContainerService(rangeStore))
        .addService(new GrpcMetaRangeService(rangeStore))
        .build();
    }
  }

  @VisibleForTesting
  Server getGrpcServer() {
    return grpcServer;
  }

  @Override
  protected void doStart() {
    try {
      grpcServer.start();
    } catch (IOException e) {
      log.error("Failed to start grpc server", e);
      throw new StorageServerRuntimeException("Failed to start grpc server", e);
    }
  }

  @Override
  protected void doStop() {
    grpcServer.shutdown();
  }

  @Override
  protected void doClose() throws IOException {}

}
