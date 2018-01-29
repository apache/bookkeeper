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
import org.apache.bookkeeper.stream.storage.api.RangeStore;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerManagerFactory;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.RangeStoreImpl;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link RangeStoreBuilder}.
 */
public class TestRangeStoreBuilder {

  private MVCCStoreFactory storeFactory;
  private final URI uri = URI.create("distributedlog://127.0.0.1/stream/storage");

  @Before
  public void setup() {
    this.storeFactory = mock(MVCCStoreFactory.class);
  }

  @Test(expected = NullPointerException.class)
  public void testBuildNullConfiguration() {
    RangeStoreBuilder.newBuilder()
      .withStorageConfiguration(null)
      .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
      .withStorageResources(StorageResources.create())
      .withRangeStoreFactory(storeFactory)
      .withDefaultBackendUri(uri)
      .build();
  }

  @Test(expected = NullPointerException.class)
  public void testBuildNullResources() {
    RangeStoreBuilder.newBuilder()
      .withStorageConfiguration(mock(StorageConfiguration.class))
      .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
      .withStorageResources(null)
      .withRangeStoreFactory(storeFactory)
      .withDefaultBackendUri(uri)
      .build();
  }

  @Test(expected = NullPointerException.class)
  public void testBuildNullRGManagerFactory() {
    RangeStoreBuilder.newBuilder()
      .withStorageConfiguration(mock(StorageConfiguration.class))
      .withStorageContainerManagerFactory(null)
      .withStorageResources(StorageResources.create())
      .withRangeStoreFactory(storeFactory)
      .withDefaultBackendUri(uri)
      .build();
  }

  @Test(expected = NullPointerException.class)
  public void testBuildNullStoreFactory() {
    RangeStoreBuilder.newBuilder()
      .withStorageConfiguration(mock(StorageConfiguration.class))
      .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
      .withStorageResources(StorageResources.create())
      .withRangeStoreFactory(null)
      .withDefaultBackendUri(uri)
      .build();
  }

  @Test(expected = NullPointerException.class)
  public void testBuildNullDefaultBackendUri() {
    RangeStoreBuilder.newBuilder()
      .withStorageConfiguration(mock(StorageConfiguration.class))
      .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
      .withStorageResources(StorageResources.create())
      .withRangeStoreFactory(storeFactory)
      .withDefaultBackendUri(null)
      .build();
  }

  @Test
  public void testBuild() {
    RangeStore rangeStore = RangeStoreBuilder.newBuilder()
      .withStorageConfiguration(mock(StorageConfiguration.class))
      .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
      .withStorageResources(StorageResources.create())
      .withRangeStoreFactory(storeFactory)
      .withDefaultBackendUri(uri)
      .build();
    assertTrue(rangeStore instanceof RangeStoreImpl);
  }

}
