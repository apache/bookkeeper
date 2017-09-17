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

package org.apache.distributedlog.stream.storage.api;

import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.distributedlog.stream.storage.api.metadata.RangeStoreService;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainerRoutingService;

/**
 * The umbrella interface for accessing ranges (both metadata and data).
 */
public interface RangeStore extends LifecycleComponent, RangeStoreService {

  /**
   * Get the routing service.
   *
   * @return routing service.
   */
  StorageContainerRoutingService getRoutingService();

  /**
   * Choose the executor for a given {@code key}.
   *
   * @param key submit key
   * @return executor
   */
  ScheduledExecutorService chooseExecutor(long key);

}
