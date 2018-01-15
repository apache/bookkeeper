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

import io.grpc.HandlerRegistry;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.stream.proto.common.Endpoint;
import org.apache.distributedlog.stream.server.conf.StorageServerConfiguration;
import org.apache.distributedlog.stream.storage.api.RangeStore;
import org.inferred.freebuilder.FreeBuilder;

/**
 * Spec for building a grpc server.
 */
@FreeBuilder
public interface GrpcServerSpec {

  /**
   * Get the store supplier for building grpc server.
   *
   * @return store supplier for building grpc server.
   */
  Supplier<RangeStore> storeSupplier();

  /**
   * Get the storage server configuration.
   *
   * @return storage server configuration.
   */
  StorageServerConfiguration storeServerConf();

  /**
   * Get the grpc endpoint.
   * @return grpc endpoint.
   */
  Optional<Endpoint> endpoint();

  /**
   * Get the stats logger.
   *
   * @return stats logger.
   */
  StatsLogger statsLogger();

  /**
   * Get the local server name.
   *
   * @return local server name.
   */
  Optional<String> localServerName();

  /**
   * Get the local handler registry.
   *
   * @return local handler registry.
   */
  Optional<HandlerRegistry> localHandlerRegistry();

  /**
   * Builder to build grpc server spec.
   */
  class Builder extends GrpcServerSpec_Builder {}

  static Builder newBuilder() {
    return new Builder();
  }

}
