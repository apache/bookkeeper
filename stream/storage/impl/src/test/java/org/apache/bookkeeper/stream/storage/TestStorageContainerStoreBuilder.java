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

package org.apache.bookkeeper.stream.storage;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.URI;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.stream.storage.api.StorageContainerStore;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerManagerFactory;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.StorageContainerStoreImpl;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link StorageContainerStoreBuilder}.
 */
public class TestStorageContainerStoreBuilder {

    private MVCCStoreFactory storeFactory;
    private final URI uri = URI.create("distributedlog://127.0.0.1/stream/storage");

    @Before
    public void setup() {
        this.storeFactory = mock(MVCCStoreFactory.class);
    }

    @Test(expected = NullPointerException.class)
    public void testBuildNullConfiguration() {
        StorageContainerStoreBuilder.newBuilder()
            .withStorageConfiguration(null)
            .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
            .withStorageResources(StorageResources.create())
            .withRangeStoreFactory(storeFactory)
            .withDefaultBackendUri(uri)
            .withStorageServerClientManager(() -> mock(StorageServerClientManager.class))
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildNullResources() {
        StorageContainerStoreBuilder.newBuilder()
            .withStorageConfiguration(mock(StorageConfiguration.class))
            .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
            .withStorageResources(null)
            .withRangeStoreFactory(storeFactory)
            .withStorageServerClientManager(() -> mock(StorageServerClientManager.class))
            .withDefaultBackendUri(uri)
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildNullRGManagerFactory() {
        StorageContainerStoreBuilder.newBuilder()
            .withStorageConfiguration(mock(StorageConfiguration.class))
            .withStorageContainerManagerFactory(null)
            .withStorageResources(StorageResources.create())
            .withRangeStoreFactory(storeFactory)
            .withStorageServerClientManager(() -> mock(StorageServerClientManager.class))
            .withDefaultBackendUri(uri)
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildNullStoreFactory() {
        StorageContainerStoreBuilder.newBuilder()
            .withStorageConfiguration(mock(StorageConfiguration.class))
            .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
            .withStorageResources(StorageResources.create())
            .withRangeStoreFactory(null)
            .withStorageServerClientManager(() -> mock(StorageServerClientManager.class))
            .withDefaultBackendUri(uri)
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildNullDefaultBackendUri() {
        StorageContainerStoreBuilder.newBuilder()
            .withStorageConfiguration(mock(StorageConfiguration.class))
            .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
            .withStorageResources(StorageResources.create())
            .withRangeStoreFactory(storeFactory)
            .withStorageServerClientManager(() -> mock(StorageServerClientManager.class))
            .withDefaultBackendUri(null)
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildStorageServerClientManager() {
        StorageContainerStoreBuilder.newBuilder()
            .withStorageConfiguration(mock(StorageConfiguration.class))
            .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
            .withStorageResources(StorageResources.create())
            .withRangeStoreFactory(storeFactory)
            .withStorageServerClientManager(null)
            .withDefaultBackendUri(uri)
            .build();
    }

    @Test
    public void testBuild() {
        StorageContainerStore storageContainerStore = StorageContainerStoreBuilder.newBuilder()
            .withStorageConfiguration(mock(StorageConfiguration.class))
            .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
            .withStorageResources(StorageResources.create())
            .withRangeStoreFactory(storeFactory)
            .withStorageServerClientManager(() -> mock(StorageServerClientManager.class))
            .withDefaultBackendUri(uri)
            .build();
        assertTrue(storageContainerStore instanceof StorageContainerStoreImpl);
    }

}
