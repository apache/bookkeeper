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

package org.apache.distributedlog.stream.storage;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.distributedlog.stream.client.internal.api.RangeServerClientManager;
import org.apache.distributedlog.stream.storage.api.RangeStore;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainerManagerFactory;
import org.apache.distributedlog.stream.storage.conf.StorageConfiguration;
import org.apache.distributedlog.stream.storage.impl.RangeStoreImpl;
import org.junit.Test;

/**
 * Unit test for {@link RangeStoreBuilder}.
 */
public class TestRangeStoreBuilder {

  @Test(expected = NullPointerException.class)
  public void testBuildNullConfiguration() {
    RangeStoreBuilder.newBuilder()
      .withStorageConfiguration(null)
      .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
      .withStorageResources(StorageResources.create())
      .withClientManagerSupplier(() -> mock(RangeServerClientManager.class))
      .build();
  }

  @Test(expected = NullPointerException.class)
  public void testBuildNullResources() {
    RangeStoreBuilder.newBuilder()
      .withStorageConfiguration(mock(StorageConfiguration.class))
      .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
      .withStorageResources(null)
      .withClientManagerSupplier(() -> mock(RangeServerClientManager.class))
      .build();
  }

  @Test(expected = NullPointerException.class)
  public void testBuildNullRGManagerFactory() {
    RangeStoreBuilder.newBuilder()
      .withStorageConfiguration(mock(StorageConfiguration.class))
      .withStorageContainerManagerFactory(null)
      .withStorageResources(StorageResources.create())
      .withClientManagerSupplier(() -> mock(RangeServerClientManager.class))
      .build();
  }

  @Test(expected = NullPointerException.class)
  public void testBuildNullClientManager() {
    RangeStoreBuilder.newBuilder()
      .withStorageConfiguration(mock(StorageConfiguration.class))
      .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
      .withStorageResources(StorageResources.create())
      .withClientManagerSupplier(null)
      .build();
  }

  @Test
  public void testBuild() {
    RangeStore rangeStore = RangeStoreBuilder.newBuilder()
      .withStorageConfiguration(mock(StorageConfiguration.class))
      .withStorageContainerManagerFactory(mock(StorageContainerManagerFactory.class))
      .withStorageResources(StorageResources.create())
      .withClientManagerSupplier(() -> mock(RangeServerClientManager.class))
      .build();
    assertTrue(rangeStore instanceof RangeStoreImpl);
  }

}
