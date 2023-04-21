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
package org.apache.bookkeeper.stream.protocol.util;

/**
 * Placement policy to place ranges to group.
 */
@FunctionalInterface
public interface StorageContainerPlacementPolicy {

    /**
     * Factory to create placement policy.
     */
    @FunctionalInterface
    interface Factory {

        /**
         * Create a placement policy to place ranges to storage containers.
         *
         * @return a new placement policy instance.
         */
        StorageContainerPlacementPolicy newPlacementPolicy();

    }

    /**
     * Placement a stream range (<tt>streamId</tt>/<tt>rangeId</tt>) to
     * a storage container.
     *
     * @param streamId stream id
     * @param rangeId range id
     * @return storage container id
     */
    long placeStreamRange(long streamId, long rangeId);

}
