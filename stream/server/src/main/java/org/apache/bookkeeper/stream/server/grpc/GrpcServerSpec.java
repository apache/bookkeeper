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

import io.grpc.HandlerRegistry;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.server.conf.StorageServerConfiguration;
import org.apache.bookkeeper.stream.storage.api.StorageContainerStore;

/**
 * Spec for building a grpc server.
 */
@Setter
@Getter
@Builder
@Accessors(fluent = true)
public class GrpcServerSpec {

    /**
     * Get the store supplier for building grpc server.
     *
     * @return store supplier for building grpc server.
     */
    Supplier<StorageContainerStore> storeSupplier;

    /**
     * Get the storage server configuration.
     *
     * @return storage server configuration.
     */
    StorageServerConfiguration storeServerConf;

    /**
     * Get the grpc endpoint.
     *
     * @return grpc endpoint.
     */
    Endpoint endpoint;

    /**
     * Get the stats logger.
     *
     * @return stats logger.
     */
    StatsLogger statsLogger;

    /**
     * Get the local server name.
     *
     * @return local server name.
     */
    String localServerName;

    /**
     * Get the local handler registry.
     *
     * @return local handler registry.
     */
    HandlerRegistry localHandlerRegistry;

}
