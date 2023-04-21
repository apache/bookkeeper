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

package org.apache.bookkeeper.stream.storage.api;

import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.grpc.proxy.ChannelFinder;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerRegistry;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerRoutingService;

/**
 * The umbrella interface for accessing storage containers.
 */
public interface StorageContainerStore extends LifecycleComponent, ChannelFinder {

    /**
     * Get the routing service.
     *
     * @return routing service.
     */
    StorageContainerRoutingService getRoutingService();

    /**
     * Get the container registry.
     *
     * @return container registry.
     */
    StorageContainerRegistry getRegistry();

}
