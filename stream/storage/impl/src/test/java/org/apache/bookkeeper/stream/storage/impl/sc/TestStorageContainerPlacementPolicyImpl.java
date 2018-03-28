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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test of {@link StorageContainerPlacementPolicyImpl}.
 */
public class TestStorageContainerPlacementPolicyImpl {

    @Test
    public void testPlacement() {
        int numStorageContainers = 1024;
        StorageContainerPlacementPolicyImpl placementPolicy =
            StorageContainerPlacementPolicyImpl.of(numStorageContainers);
        assertEquals(numStorageContainers, placementPolicy.getNumStorageContainers());
        long scId = placementPolicy.placeStreamRange(1234L, 5678L);
        assertTrue(scId >= 0 && scId < placementPolicy.getNumStorageContainers());
    }

}
