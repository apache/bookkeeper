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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;
import org.apache.commons.configuration.CompositeConfiguration;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link DefaultStorageContainerFactory}.
 */
public class TestDefaultStorageContainerFactory {

    @Test
    public void testCreate() throws Exception {
        OrderedScheduler scheduler = mock(OrderedScheduler.class);
        OrderedScheduler snapshotScheduler = mock(OrderedScheduler.class);
        MVCCStoreFactory storeFactory = mock(MVCCStoreFactory.class);
        ListeningScheduledExecutorService snapshotExecutor = mock(ListeningScheduledExecutorService.class);
        when(snapshotScheduler.chooseThread(anyLong())).thenReturn(snapshotExecutor);
        Mockito.doReturn(mock(ListenableScheduledFuture.class))
            .when(snapshotExecutor).scheduleWithFixedDelay(
            any(Runnable.class), anyInt(), anyInt(), any(TimeUnit.class));


        DefaultStorageContainerFactory factory = new DefaultStorageContainerFactory(
            new StorageConfiguration(new CompositeConfiguration()),
            (streamId, rangeId) -> streamId,
            scheduler,
            storeFactory,
            URI.create("distributedlog://127.0.0.1/stream/storage"));
        StorageContainer sc = factory.createStorageContainer(1234L);
        assertTrue(sc instanceof StorageContainerImpl);
        assertEquals(1234L, sc.getId());
    }
}
