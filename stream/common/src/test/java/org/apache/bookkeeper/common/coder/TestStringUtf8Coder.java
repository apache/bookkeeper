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
 * Unit Tests for {@link StringUtf8Coder}.
 */
public class TestStringUtf8Coder extends CoderBasicTestCase {

    private static final Coder<String> TEST_CODER = StringUtf8Coder.of();

    private static final List<String> TEST_VALUES = Arrays.asList(
        "", "a", "13", "hello",
        "a longer string with spaces and all that",
        "a string with a \n newline",
        "スタリング");

    @Test
    public void testDecodeEncodeEquals() throws Exception {
        for (String value : TEST_VALUES) {
            coderDecodeEncodeEqual(TEST_CODER, value);
        }
    }

}
