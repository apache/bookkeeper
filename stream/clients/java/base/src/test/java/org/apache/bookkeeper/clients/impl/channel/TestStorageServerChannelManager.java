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

package org.apache.bookkeeper.clients.impl.channel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.junit.After;
import org.junit.Test;

/**
 * Unit test for {@link StorageServerChannelManager}.
 */
public class TestStorageServerChannelManager {

    private final Endpoint endpoint1 = Endpoint.newBuilder()
        .setHostname("127.0.0.1")
        .setPort(80)
        .build();
    private final StorageServerChannel channel1 = mock(StorageServerChannel.class);
    private final Endpoint endpoint2 = Endpoint.newBuilder()
        .setHostname("127.0.0.2")
        .setPort(8080)
        .build();
    private final StorageServerChannel channel2 = mock(StorageServerChannel.class);
    private final Endpoint endpoint3 = Endpoint.newBuilder()
        .setHostname("127.0.0.3")
        .setPort(8181)
        .build();

    private final StorageServerChannelManager channelManager =
        new StorageServerChannelManager((endpoint) -> {
            if (endpoint == endpoint1) {
                return channel1;
            } else if (endpoint == endpoint2) {
                return channel2;
            } else {
                return mock(StorageServerChannel.class);
            }
        });

    @After
    public void tearDown() {
        channelManager.close();
    }

    @Test
    public void testGetNullChannel() {
        assertNull(channelManager.getChannel(endpoint1));
    }

    @Test
    public void testGetOrCreateChannel() {
        StorageServerChannel channel = channelManager.getOrCreateChannel(endpoint1);
        assertTrue(channel == channel1);
        channel = channelManager.getOrCreateChannel(endpoint2);
        assertTrue(channel == channel2);
        channel = channelManager.getOrCreateChannel(endpoint3);
        assertTrue(channel != channel1 && channel != channel2);
        assertEquals(3, channelManager.getNumChannels());
        channelManager.close();
        assertEquals(0, channelManager.getNumChannels());
        verify(channel1, times(1)).close();
        verify(channel2, times(1)).close();
        verify(channel, times(1)).close();
    }

    @Test
    public void testGetOrCreateChannelAfterClosed() {
        channelManager.close();
        assertNull(channelManager.getOrCreateChannel(endpoint1));
        assertFalse(channelManager.contains(endpoint1));
        assertEquals(0, channelManager.getNumChannels());
    }

    @Test
    public void testAddRangeServer() {
        StorageServerChannel ch1 = mock(StorageServerChannel.class);
        StorageServerChannel ch2 = mock(StorageServerChannel.class);
        assertNull(channelManager.getChannel(endpoint1));
        assertTrue(channelManager.addStorageServer(endpoint1, ch1));
        assertTrue(ch1 == channelManager.getChannel(endpoint1));
        assertEquals(1, channelManager.getNumChannels());
        assertFalse(channelManager.addStorageServer(endpoint1, ch2));
        assertTrue(ch1 == channelManager.getChannel(endpoint1));
        assertEquals(1, channelManager.getNumChannels());
        verify(ch2, times(1)).close();
    }

    @Test
    public void testAddRangeServerAfterClosed() {
        channelManager.close();
        StorageServerChannel ch1 = mock(StorageServerChannel.class);
        assertNull(channelManager.getChannel(endpoint1));
        assertFalse(channelManager.addStorageServer(endpoint1, ch1));
        assertNull(channelManager.getChannel(endpoint1));
        assertEquals(0, channelManager.getNumChannels());
        verify(ch1, times(1)).close();
    }

    @Test
    public void testRemoveChannel() {
        StorageServerChannel ch = mock(StorageServerChannel.class);
        assertNull(channelManager.getChannel(endpoint1));
        assertTrue(channelManager.addStorageServer(endpoint1, ch));
        assertTrue(ch == channelManager.getChannel(endpoint1));
        assertEquals(1, channelManager.getNumChannels());
        assertTrue(ch == channelManager.removeChannel(endpoint1, null));
        verify(ch, times(1)).close();
        assertEquals(0, channelManager.getNumChannels());
    }

    @Test
    public void testRemoveChannelAfterClosed() {
        channelManager.close();
        assertNull(channelManager.removeChannel(endpoint1, null));
    }

    @Test
    public void testConditionalRemoveChannelSuccess() {
        StorageServerChannel ch1 = mock(StorageServerChannel.class);
        assertNull(channelManager.getChannel(endpoint1));
        assertTrue(channelManager.addStorageServer(endpoint1, ch1));
        assertTrue(ch1 == channelManager.getChannel(endpoint1));
        assertEquals(1, channelManager.getNumChannels());
        assertTrue(ch1 == channelManager.removeChannel(endpoint1, ch1));
        verify(ch1, times(1)).close();
        assertEquals(0, channelManager.getNumChannels());
    }

    @Test
    public void testConditionalRemoveChannelFailure() {
        StorageServerChannel ch1 = mock(StorageServerChannel.class);
        StorageServerChannel ch2 = mock(StorageServerChannel.class);
        assertNull(channelManager.getChannel(endpoint1));
        assertTrue(channelManager.addStorageServer(endpoint1, ch1));
        assertTrue(ch1 == channelManager.getChannel(endpoint1));
        assertEquals(1, channelManager.getNumChannels());
        assertNull(channelManager.removeChannel(endpoint1, ch2));
        verify(ch1, times(0)).close();
        verify(ch2, times(0)).close();
        assertEquals(1, channelManager.getNumChannels());
    }

}
