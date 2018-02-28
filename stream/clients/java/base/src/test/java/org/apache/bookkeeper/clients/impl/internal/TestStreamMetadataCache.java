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

package org.apache.bookkeeper.clients.impl.internal;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.clients.impl.internal.api.RootRangeClient;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.junit.Test;

/**
 * Unit test of {@link StreamMetadataCache}.
 */
public class TestStreamMetadataCache {

    private final RootRangeClient scClient = mock(RootRangeClient.class);
    private final StreamMetadataCache cache = new StreamMetadataCache(scClient);
    private final StreamProperties props = StreamProperties.newBuilder()
        .setStorageContainerId(1234L)
        .setStreamId(2345L)
        .setStreamName("test-stream")
        .setStreamConf(DEFAULT_STREAM_CONF)
        .build();

    @Test
    public void testGetStreamProperties() throws Exception {
        when(scClient.getStream(anyLong()))
            .thenReturn(FutureUtils.value(props));
        assertEquals(0, cache.getStreams().size());
        assertEquals(props, FutureUtils.result(cache.getStreamProperties(1234L)));
        assertEquals(1, cache.getStreams().size());
        verify(scClient, times(1)).getStream(eq(1234L));
    }

    @Test
    public void testPutStreamProperties() throws Exception {
        assertEquals(0, cache.getStreams().size());
        assertTrue(cache.putStreamProperties(1234L, props));
        assertEquals(1, cache.getStreams().size());
        assertFalse(cache.putStreamProperties(1234L, props));
        assertEquals(1, cache.getStreams().size());
    }

}
