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
import java.util.concurrent.ThreadLocalRandom;
import org.apache.bookkeeper.stream.protocol.util.StorageContainerPlacementPolicy;

/**
 * The default implementation of {@link StorageContainerPlacementPolicy}.
 */
public class StorageContainerPlacementPolicyImpl implements StorageContainerPlacementPolicy {

    public static StorageContainerPlacementPolicyImpl of(int numStorageContainers) {
        return new StorageContainerPlacementPolicyImpl(numStorageContainers);
    }

    private final int numStorageContainers;

    private StorageContainerPlacementPolicyImpl(int numStorageContainers) {
        this.numStorageContainers = numStorageContainers;
    }

    @VisibleForTesting
    long getNumStorageContainers() {
        return numStorageContainers;
    }

    @Override
    public long placeStreamRange(long streamId, long rangeId) {
        return ThreadLocalRandom.current().nextInt(numStorageContainers);
    }
}
