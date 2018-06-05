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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerFactory;
import org.apache.bookkeeper.stream.storage.exceptions.StorageException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link StorageContainerRegistryImpl}.
 */
public class TestStorageContainerRegistryImpl {

    private OrderedScheduler scheduler;

    @Before
    public void setUp() {
        this.scheduler = OrderedScheduler.newSchedulerBuilder()
            .numThreads(1)
            .name("test-storage-container-registry-impl")
            .build();
    }

    @After
    public void tearDown() {
        if (null != this.scheduler) {
            this.scheduler.shutdown();
        }
    }

    private StorageContainer createStorageContainer() {
        StorageContainer sc = mock(StorageContainer.class);
        when(sc.start()).thenReturn(FutureUtils.value(sc));
        when(sc.stop()).thenReturn(FutureUtils.value(null));
        return sc;
    }

    private StorageContainerFactory createStorageContainerFactory() {
        return scId -> createStorageContainer();
    }

    @Test
    public void testOperationsAfterClosed() throws Exception {
        StorageContainerFactory scFactory = createStorageContainerFactory();
        StorageContainerRegistryImpl registry = new StorageContainerRegistryImpl(scFactory);
        registry.close();

        long scId = 1234L;

        try {
            FutureUtils.result(registry.startStorageContainer(scId));
            fail("Should fail to start storage container after registry is closed");
        } catch (ObjectClosedException oce) {
            // expected
            assertEquals(0, registry.getNumStorageContainers());
        }

        try {
            FutureUtils.result(registry.stopStorageContainer(scId));
            fail("Should fail to start storage container after registry is closed");
        } catch (ObjectClosedException oce) {
            // expected
            assertEquals(0, registry.getNumStorageContainers());
        }
    }

    @Test
    public void testStopNotFoundStorageContainer() throws Exception {
        StorageContainerFactory scFactory = createStorageContainerFactory();
        StorageContainerRegistryImpl registry = new StorageContainerRegistryImpl(scFactory);
        FutureUtils.result(registry.stopStorageContainer(1234L));
        assertEquals(0, registry.getNumStorageContainers());
    }

    @Test
    public void testStartStorageContainerTwice() throws Exception {
        StorageContainerFactory scFactory = createStorageContainerFactory();
        StorageContainerRegistryImpl registry = new StorageContainerRegistryImpl(scFactory);
        FutureUtils.result(registry.startStorageContainer(1234L));
        assertEquals(1, registry.getNumStorageContainers());
        // second time
        try {
            FutureUtils.result(registry.startStorageContainer(1234L));
            fail("Should fail on starting same storage container twice");
        } catch (StorageException ue) {
            assertEquals(1, registry.getNumStorageContainers());
        }
    }

    @Test
    public void testStartStopStorageContainers() throws Exception {
        StorageContainer sc1 = createStorageContainer();
        StorageContainer sc2 = createStorageContainer();
        StorageContainerFactory factory = scId -> {
            if (scId == 1L) {
                return sc1;
            } else {
                return sc2;
            }
        };

        long scId = 1L;

        StorageContainerRegistryImpl registry = new StorageContainerRegistryImpl(factory);
        FutureUtils.result(registry.startStorageContainer(scId));
        assertEquals(1, registry.getNumStorageContainers());
        assertEquals(sc1, registry.getStorageContainer(scId));

        scId = 2L;
        FutureUtils.result(registry.startStorageContainer(scId));
        assertEquals(2, registry.getNumStorageContainers());
        assertEquals(sc1, registry.getStorageContainer(1L));
        assertEquals(sc2, registry.getStorageContainer(2L));

        FutureUtils.result(registry.stopStorageContainer(scId));
        assertEquals(1, registry.getNumStorageContainers());
        assertEquals(sc1, registry.getStorageContainer(1L));

        registry.close();
        verify(sc1, times(1)).close();
        assertEquals(0, registry.getNumStorageContainers());

        // double close
        registry.close();
        verify(sc1, times(1)).close();
    }

}
