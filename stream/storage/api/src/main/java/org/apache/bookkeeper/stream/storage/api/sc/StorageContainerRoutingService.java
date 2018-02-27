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

import org.apache.bookkeeper.stream.proto.common.Endpoint;

/**
 * A service for routing requests.
 */
public interface StorageContainerRoutingService {

    /**
     * Get the location for a storage container.
     *
     * @return location for a storage container.
     */
    Endpoint getStorageContainer(long scId);

}
