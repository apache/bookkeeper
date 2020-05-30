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

package org.apache.bookkeeper.stream.storage.api.sc;

import java.util.concurrent.CompletableFuture;

/**
 * A <i>Registry</i> for Storage Containers.
 */
public interface StorageContainerRegistry extends AutoCloseable {

    /**
     * Gets the number of registered storage containers.
     *
     * @return the number of registered storage containers.
     */
    int getNumStorageContainers();

    /**
     * Get the instance of storage container {@code storageContainerId}.
     *
     * @return the instance of the storage container
     */
    StorageContainer getStorageContainer(long storageContainerId);

    /**
     * Get the instance of storage container {@code storageContainerId}.
     *
     * @param storageContainerId storage container id
     * @param defaultContainer the default container to return if the container doesn't exist in the registry
     * @return the instance of the storage container.
     */
    StorageContainer getStorageContainer(long storageContainerId, StorageContainer defaultContainer);

    /**
     * Start the storage container in this registry.
     *
     * @param scId storage container id
     * @return a future represents the started storage container or exception if failed to start.
     */
    CompletableFuture<StorageContainer> startStorageContainer(long scId);

    /**
     * Stop the storage container in this registry blindly.
     *
     * @param scId storage container id
     * @return a future represents the result of stopping a storage container or exception if failed to start.
     */
    default CompletableFuture<Void> stopStorageContainer(long scId) {
        return stopStorageContainer(scId, null);
    }

    /**
     * Stop the storage container in this registry.
     *
     * @param scId storage container id
     * @param container storage container instance to stop
     * @return a future represents the result of stopping a storage container or exception if failed to start.
     */
    CompletableFuture<Void> stopStorageContainer(long scId, StorageContainer container);

    /**
     * Close the registry.
     */
    @Override
    void close();
}
