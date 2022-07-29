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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("javadoc")
public class AbstractStatefulHashTest {

    private AbstractStatefulHash hash;

    public AbstractStatefulHashTest() {
        this.hash = mock(AbstractStatefulHash.class, Mockito.CALLS_REAL_METHODS);
    }

    @Test
    public void testUpdateByteArray() {
        final byte[] input = new byte[42];

        hash.update(input);

        verify(hash, times(1))
            .updateUnchecked(eq(input), eq(0), eq(input.length));
    }

    @Test
    public void testUpdateByteArrayIntInt() {
        final byte[] input = new byte[42];

        hash.update(input, 5, 10);

        verify(hash, times(1))
            .updateUnchecked(eq(input), eq(5), eq(10));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateByteArrayIntNegInt() {
        final byte[] input = new byte[42];

        hash.update(input, 1, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testUpdateByteArrayNegIntInt() {
        final byte[] input = new byte[42];

        hash.update(input, -1, 10);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testUpdateByteArrayIntIntOverflow() {
        final byte[] input = new byte[42];

        hash.update(input, 40, 3);
    }

    @Test
    public void testUpdateByteBuffer() {
        final ByteBuffer input = ByteBuffer.allocate(20);
        input.position(5);
        input.limit(15);

        hash.update(input);
        assertEquals(input.limit(), input.position());

        verify(hash, times(1))
            .updateUnchecked(eq(input.array()), eq(input.arrayOffset() + 5), eq(10));
    }

    @Test
    public void testUpdateReadOnlyByteBuffer() {
        final ByteBuffer input = ByteBuffer.allocate(20).asReadOnlyBuffer();
        input.position(5);
        input.limit(15);

        hash.update(input);
        assertEquals(input.limit(), input.position());

        verify(hash, times(1))
            .updateUnchecked(any(byte[].class), eq(0), eq(10));
    }

    @Test
    public void testGetBytes() {
        final List<byte[]> captures = new ArrayList<>();

        when(hash.length()).thenReturn(5);

        doAnswer(invocationOnMock -> {
            captures.add(invocationOnMock.getArgument(0));
            return invocationOnMock.callRealMethod();
        }).when(hash).writeBytes(any(byte[].class), eq(0), eq(5));

        hash.getBytes();
        assertEquals(5, captures.get(0).length);
    }

    @Test
    public void testGetBytesByteArrayInt() {
        final byte[] output = new byte[5];

        when(hash.length()).thenReturn(output.length);
        when(hash.getLong()).thenReturn(0x1234567890L);

        hash.getBytes(output, 0, output.length);
        assertArrayEquals(new byte[] { (byte) 0x90, 0x78, 0x56, 0x34, 0x12 }, output);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetBytesByteArrayNegInt() {
        final byte[] output = new byte[5];

        when(hash.length()).thenReturn(output.length);

        hash.getBytes(output, -1, output.length);

        verify(hash, atLeast(0)).length();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetBytesByteArrayIntOverflow() {
        final byte[] output = new byte[5];

        hash.getBytes(output, 0, output.length + 1);
    }

    @Test
    public void testGetBytesByteArrayIntPartial() {
        final byte[] output = new byte[5];

        when(hash.length()).thenReturn(output.length + 1);

        hash.getBytes(output, 0, output.length);

        verify(hash, times(1)).writeBytes(eq(output), eq(0), eq(output.length));
    }

    @Test
    public void testGetByte() {
        when(hash.getInt()).thenReturn(0x12345678);

        assertEquals(0x78, hash.getByte());
    }

    @Test
    public void testGetShort() {
        when(hash.getInt()).thenReturn(0x12345678);

        assertEquals(0x5678, hash.getShort());
    }
}
