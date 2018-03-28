/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.scurrilous.circe.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scurrilous.circe.StatefulHash;
import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("javadoc")
public class AbstractIncrementalIntHashTest {

    private AbstractIncrementalIntHash hash;

    @Before
    public void setup() {
        this.hash = mock(AbstractIncrementalIntHash.class, Mockito.CALLS_REAL_METHODS);
    }

    @Test
    public void testAsStateful() {
        final byte[] input = new byte[10];

        when(hash.initial()).thenReturn(42);
        when(hash.resumeUnchecked(eq(42), eq(input), eq(2), eq(4))).thenReturn(99);

        StatefulHash stateful = hash.createStateful();
        stateful.algorithm();
        stateful.length();
        assertNotSame(stateful, stateful.createNew());
        stateful.reset();
        stateful.update(input, 2, 4);
        assertEquals(99, stateful.getInt());

        // verification
        verify(hash, times(1)).algorithm();
        verify(hash, times(1)).length();
        verify(hash, times(1)).initial();
        verify(hash, times(1)).resumeUnchecked(
            eq(42), eq(input), eq(2), eq(4));
    }

    @Test
    public void testCalculateByteArray() {
        final byte[] input = new byte[10];

        when(hash.initial()).thenReturn(42);

        hash.calculate(input);

        verify(hash, times(1)).initial();
        verify(hash, times(1)).resume(eq(42), eq(input));
    }

    @Test
    public void testCalculateByteArrayIntInt() {
        final byte[] input = new byte[10];

        when(hash.initial()).thenReturn(42);

        hash.calculate(input, 2, 4);

        verify(hash, times(1)).initial();
        verify(hash, times(1)).resume(eq(42), eq(input), eq(2), eq(4));
    }

    @Test
    public void testCalculateByteBuffer() {
        final ByteBuffer input = ByteBuffer.allocate(10);

        when(hash.initial()).thenReturn(42);

        hash.calculate(input);

        verify(hash, times(1)).initial();
        verify(hash, times(1)).resume(eq(42), eq(input));
    }

    @Test
    public void testResumeIntByteArray() {
        final byte[] input = new byte[10];

        hash.resume(42, input);

        verify(hash, times(1)).resumeUnchecked(eq(42), eq(input), eq(0), eq(input.length));
    }

    @Test
    public void testResumeIntByteArrayIntInt() {
        final byte[] input = new byte[10];

        hash.resume(42, input, 2, 4);

        verify(hash, times(1)).resumeUnchecked(eq(42), eq(input), eq(2), eq(4));
    }

    @Test
    public void testResumeIntByteBuffer() {
        final ByteBuffer input = ByteBuffer.allocate(20);
        input.position(5);
        input.limit(15);

        hash.resume(42, input);
        assertEquals(input.limit(), input.position());

        verify(hash, times(1))
            .resumeUnchecked(eq(42), eq(input.array()), eq(input.arrayOffset() + 5), eq(10));
    }

    @Test
    public void testResumeIntReadOnlyByteBuffer() {
        final ByteBuffer input = ByteBuffer.allocate(20).asReadOnlyBuffer();
        input.position(5);
        input.limit(15);

        hash.resume(42, input);
        assertEquals(input.limit(), input.position());

        verify(hash, times(1))
            .resumeUnchecked(eq(42), any(byte[].class), eq(0), eq(10));
    }
}
