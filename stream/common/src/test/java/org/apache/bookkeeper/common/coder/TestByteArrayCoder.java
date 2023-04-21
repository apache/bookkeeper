/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.common.coder;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests for {@link ByteArrayCoder}.
 */
public class TestByteArrayCoder extends CoderBasicTestCase {

    private static final ByteArrayCoder TEST_CODER = ByteArrayCoder.of();

    private static final List<byte[]> TEST_VALUES = Arrays.asList(
        new byte[]{0xa, 0xb, 0xc},
        new byte[]{0xd, 0x3},
        new byte[]{0xd, 0xe},
        new byte[]{});

    @Test
    public void testDecodeEncodeEquals() throws Exception {
        for (byte[] value : TEST_VALUES) {
            coderDecodeEncodeEqual(TEST_CODER, value);
        }
    }

    @Test
    public void testEncodeThenMutate() throws Exception {
        byte[] input = {0x7, 0x3, 0xA, 0xf};
        byte[] encoded = encode(TEST_CODER, input);
        input[1] = 0x9;
        byte[] decoded = decode(TEST_CODER, encoded);

        // byte array coder that does encoding/decoding without copying
        // the bytes, so mutating the input will mutate the output
        assertThat(input, equalTo(decoded));
    }

}
