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

package org.apache.distributedlog.stream.storage.impl.sc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainer;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainerFactory;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainerRegistry;
import org.apache.distributedlog.stream.storage.exceptions.StorageException;

/**
 * The default implementation of {@link StorageContainerRegistry}.
 */
@Slf4j
public class StorageContainerRegistryImpl implements StorageContainerRegistry {

  private static final String COMPONENT_NAME = StorageContainerRegistry.class.getSimpleName();

  private final StorageContainerFactory scFactory;
  private final ConcurrentMap<Long, StorageContainer> groups;
  private final ReentrantReadWriteLock closeLock;
  private final StorageContainer failRequestStorageContainer;
  private boolean closed = false;

  public StorageContainerRegistryImpl(StorageContainerFactory factory,
                                      OrderedScheduler scheduler) {
    this.scFactory = factory;
    this.failRequestStorageContainer = FailRequestStorageContainer.of(scheduler);
    this.groups = Maps.newConcurrentMap();
    this.closeLock = new ReentrantReadWriteLock();
  }

  @VisibleForTesting
  public void setStorageContainer(long scId, StorageContainer group) {
    groups.put(scId, group);
  }

  @Override
  public int getNumStorageContainers() {
    return groups.size();
  }

  @Override
  public StorageContainer getStorageContainer(long storageContainerId) {
    return groups.getOrDefault(storageContainerId, failRequestStorageContainer);
  }

  @Override
  public CompletableFuture<Void> startStorageContainer(long scId) {
    closeLock.readLock().lock();
    try {
      return unsafeStartStorageContainer(scId);
    } finally {
      closeLock.readLock().unlock();
    }
  }

  private CompletableFuture<Void> unsafeStartStorageContainer(long scId) {
    if (closed) {
      return FutureUtils.exception(new ObjectClosedException(COMPONENT_NAME));
    }

    if (groups.containsKey(scId)) {
      return FutureUtils.exception(new StorageException("StorageContainer " + scId + " already registered"));
    }

    StorageContainer newStorageContainer = scFactory.createStorageContainer(scId);
    StorageContainer oldStorageContainer = groups.putIfAbsent(scId, newStorageContainer);
    if (null != oldStorageContainer) {
      newStorageContainer.close();
      return FutureUtils.exception(new StorageException("StorageContainer " + scId + " already registered"));
    }

    log.info("Registered StorageContainer ('{}').", scId);
    return newStorageContainer.start();
  }

  @Override
  public CompletableFuture<Void> stopStorageContainer(long scId) {
    closeLock.readLock().lock();
    try {
      return unsafeStopStorageContainer(scId);
    } finally {
      closeLock.readLock().unlock();
    }
  }

  private CompletableFuture<Void> unsafeStopStorageContainer(long scId) {
    if (closed) {
      return FutureUtils.exception(new ObjectClosedException(COMPONENT_NAME));
    }

    StorageContainer group = groups.remove(scId);

    if (null == group) {
      return FutureUtils.value(null);
    }

    log.info("Unregistered StorageContainer ('{}').", scId);
    return group.stop();
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
    groups.values().forEach(StorageContainer::close);
    groups.clear();
  }
}
