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

package org.apache.bookkeeper.stream.storage.impl.sc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerFactory;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerRegistry;
import org.apache.bookkeeper.stream.storage.exceptions.StorageException;

/**
 * The default implementation of {@link StorageContainerRegistry}.
 */
@Slf4j
public class StorageContainerRegistryImpl implements StorageContainerRegistry {

    private static final String COMPONENT_NAME = StorageContainerRegistry.class.getSimpleName();

    private final StorageContainerFactory scFactory;
    private final ConcurrentMap<Long, StorageContainer> containers;
    private final ReentrantReadWriteLock closeLock;
    private boolean closed = false;

    public StorageContainerRegistryImpl(StorageContainerFactory factory) {
        this.scFactory = factory;
        this.containers = Maps.newConcurrentMap();
        this.closeLock = new ReentrantReadWriteLock();
    }

    @VisibleForTesting
    public void setStorageContainer(long scId, StorageContainer group) {
        containers.put(scId, group);
    }

    @Override
    public int getNumStorageContainers() {
        return containers.size();
    }

    @Override
    public StorageContainer getStorageContainer(long storageContainerId) {
        return getStorageContainer(storageContainerId, StorageContainer404.of());
    }

    @Override
    public StorageContainer getStorageContainer(long storageContainerId, StorageContainer defaultContainer) {
        return containers.getOrDefault(storageContainerId, defaultContainer);
    }

    @Override
    public CompletableFuture<StorageContainer> startStorageContainer(long scId) {
        closeLock.readLock().lock();
        try {
            return unsafeStartStorageContainer(scId);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    private CompletableFuture<StorageContainer> unsafeStartStorageContainer(long scId) {
        if (closed) {
            return FutureUtils.exception(new ObjectClosedException(COMPONENT_NAME));
        }

        if (containers.containsKey(scId)) {
            return FutureUtils.exception(new StorageException("StorageContainer " + scId + " already registered"));
        }

        StorageContainer newStorageContainer = scFactory.createStorageContainer(scId);
        StorageContainer oldStorageContainer = containers.putIfAbsent(scId, newStorageContainer);
        if (null != oldStorageContainer) {
            newStorageContainer.close();
            return FutureUtils.exception(new StorageException("StorageContainer " + scId + " already registered"));
        }

        log.info("Registered StorageContainer ('{}').", scId);
        return newStorageContainer.start()
            .whenComplete((container, cause) -> {
                if (null != cause) {
                    if (containers.remove(scId, newStorageContainer)) {
                        log.warn("De-registered StorageContainer ('{}') when failed to start", scId, cause);
                    } else {
                        log.warn("Fail to de-register StorageContainer ('{}') when failed to start", scId, cause);
                    }
                    log.info("Release resources hold by StorageContainer ('{}') during de-register", scId);
                    newStorageContainer.stop().exceptionally(throwable -> {
                        log.error("Stop StorageContainer ('{}') fail during de-register", scId);
                        return null;
                    });
                } else {
                    log.info("Successfully started registered StorageContainer ('{}').", scId);
                }
            });
    }

    @Override
    public CompletableFuture<Void> stopStorageContainer(long scId, StorageContainer container) {
        closeLock.readLock().lock();
        try {
            return unsafeStopStorageContainer(scId, container);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    private CompletableFuture<Void> unsafeStopStorageContainer(long scId, StorageContainer container) {
        if (closed) {
            return FutureUtils.exception(new ObjectClosedException(COMPONENT_NAME));
        }

        if (null == container) {
            StorageContainer existingContainer = containers.remove(scId);
            if (null != existingContainer) {
                log.info("Unregistered StorageContainer ('{}').", scId);
                return existingContainer.stop();
            } else {
                return FutureUtils.Void();
            }
        } else {
            boolean removed = containers.remove(scId, container);

            if (removed) {
                log.info("Unregistered StorageContainer ('{}').", scId);
            }

            // no matter we successfully removed the containers or not, we need to close the current container.
            // this ensure the resources held by the passed-in container are released.
            return container.stop();
        }
    }

    @Override
    public void close() {
        closeLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }

        // close all the ranges
        containers.values().forEach(StorageContainer::close);
        containers.clear();
    }
}
