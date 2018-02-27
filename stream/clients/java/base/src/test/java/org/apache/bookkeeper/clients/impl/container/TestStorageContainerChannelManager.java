/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients.impl.container;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

/**
 * Unit tests for {@link StorageContainerChannelManager}.
 */
public class TestStorageContainerChannelManager {

    @Test
    public void testGetOrCreate() {
        StorageContainerChannel channel1 = mock(StorageContainerChannel.class);
        StorageContainerChannel channel2 = mock(StorageContainerChannel.class);
        StorageContainerChannel channel3 = mock(StorageContainerChannel.class);

        StorageContainerChannelFactory factory = mock(StorageContainerChannelFactory.class);
        StorageContainerChannelManager manager = new StorageContainerChannelManager(factory);
        when(factory.createStorageContainerChannel(anyLong()))
            .thenReturn(channel1)
            .thenReturn(channel2)
            .thenReturn(channel3);

        assertNull(manager.remove(1L));
        assertEquals(channel1, manager.getOrCreate(1L));
        verify(factory, times(1)).createStorageContainerChannel(eq(1L));
        assertEquals(channel1, manager.getOrCreate(1L));
        verify(factory, times(1)).createStorageContainerChannel(eq(1L));

        assertEquals(channel1, manager.remove(1L));
    }

    @Test
    public void testClear() throws Exception {
        StorageContainerChannelManager manager = new StorageContainerChannelManager(
            scId -> mock(StorageContainerChannel.class));

        int numChannels = 10;
        for (int i = 0; i < numChannels; i++) {
            assertNotNull(manager.getOrCreate(i));
        }
        assertEquals(numChannels, manager.getNumChannels());
        manager.close();
        assertEquals(0, manager.getNumChannels());
    }

}
