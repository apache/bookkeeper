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

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Unit Tests for {@link VarIntCoder}.
 */
public class TestVarIntCoder extends CoderBasicTestCase {

    private static final Coder<Integer> TEST_CODER = VarIntCoder.of();

    private static final List<Integer> TEST_VALUES = Arrays.asList(
        -11, -3, -1, 0, 1, 5, 13, 29,
        Integer.MAX_VALUE,
        Integer.MIN_VALUE);

    @Test
    public void testDecodeEncodeEquals() throws Exception {
        for (Integer value : TEST_VALUES) {
            coderDecodeEncodeEqual(TEST_CODER, value);
        }
    }

}
