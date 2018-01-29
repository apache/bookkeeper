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

import java.net.URI;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerFactory;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;

/**
 * The default storage container factory for creating {@link StorageContainer}s.
 */
public class DefaultStorageContainerFactory implements StorageContainerFactory {

  private final StorageConfiguration storageConf;
  private final StorageContainerPlacementPolicy rangePlacementPolicy;
  private final OrderedScheduler scheduler;
  private final MVCCStoreFactory storeFactory;
  private final URI defaultBackendUri;

  public DefaultStorageContainerFactory(StorageConfiguration storageConf,
                                        StorageContainerPlacementPolicy rangePlacementPolicy,
                                        OrderedScheduler scheduler,
                                        MVCCStoreFactory storeFactory,
                                        URI defaultBackendUri) {
    this.storageConf = storageConf;
    this.rangePlacementPolicy = rangePlacementPolicy;
    this.scheduler = scheduler;
    this.storeFactory = storeFactory;
    this.defaultBackendUri = defaultBackendUri;
  }

  @Override
  public StorageContainer createStorageContainer(long scId) {
    return new StorageContainerImpl(
      storageConf,
      scId,
      rangePlacementPolicy,
      scheduler,
      storeFactory,
      defaultBackendUri);
  }
}
