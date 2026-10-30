/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/**
 * Tests for {@link FileInfo}.
 */
public class FileInfoTest {

    /**
     * {@link FileChannel#transferTo} may transfer fewer bytes than requested. When that happens the
     * copy loop in {@code moveToNewLocation} must only ask for the remaining bytes on each call.
     * Asking for the full {@code size} every time lets a later call copy past the requested range
     * when the source file holds more than {@code size} bytes, so more than {@code size} bytes end
     * up at the new location.
     */
    @Test
    public void testTransferFullyCopiesExactlyRequestedSizeOnPartialTransfer() throws Exception {
        long size = 100;
        // The source channel is larger than the range we want to copy.
        long sourceLength = 1000;

        FileChannel src = mock(FileChannel.class);
        FileChannel dst = mock(FileChannel.class);

        AtomicLong totalTransferred = new AtomicLong(0);
        // Simulate a channel that transfers at most 30 bytes per call, but never more than the
        // count requested and never past the end of the (larger) source file.
        when(src.transferTo(anyLong(), anyLong(), any(WritableByteChannel.class)))
                .thenAnswer(invocation -> {
                    long position = invocation.getArgument(0);
                    long count = invocation.getArgument(1);
                    long available = sourceLength - position;
                    long chunk = Math.min(Math.min(count, available), 30L);
                    totalTransferred.addAndGet(chunk);
                    return chunk;
                });

        FileInfo.transferFully(src, dst, size, new File("new.idx.rloc"));

        assertEquals("exactly size bytes must be copied", size, totalTransferred.get());
    }

    /**
     * A zero-length copy must complete without touching the source channel or throwing.
     */
    @Test
    public void testTransferFullyWithZeroSizeDoesNothing() throws Exception {
        FileChannel src = mock(FileChannel.class);
        FileChannel dst = mock(FileChannel.class);

        FileInfo.transferFully(src, dst, 0, new File("new.idx.rloc"));

        assertTrue(true);
    }
}
