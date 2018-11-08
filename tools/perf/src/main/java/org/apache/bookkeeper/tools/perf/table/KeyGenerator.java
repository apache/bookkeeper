/*
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
 */

package org.apache.bookkeeper.tools.perf.table;

import io.netty.buffer.ByteBuf;

/**
 * Util class used for perf benchmarks.
 */
final class KeyGenerator {

    private final long numKeys;
    private final long keysPerPrefix;
    private final int prefixSize;

    KeyGenerator(long numKeys,
                 long keysPerPrefix,
                 int prefixSize) {
        this.numKeys = numKeys;
        this.keysPerPrefix = keysPerPrefix;
        this.prefixSize = prefixSize;
    }

    public void generateKeyFromLong(ByteBuf slice, long n) {
        int startPos = 0;
        if (keysPerPrefix > 0) {
            long numPrefix = (numKeys + keysPerPrefix - 1) / keysPerPrefix;
            long prefix = n % numPrefix;
            int bytesToFill = Math.min(prefixSize, 8);
            for (int i = 0; i < bytesToFill; i++) {
                slice.setByte(i,  (byte) (prefix % 256));
                prefix /= 256;
            }
            for (int i = 8; i < bytesToFill; ++i) {
                slice.setByte(i, '0');
            }
            startPos = bytesToFill;
        }
        for (int i = slice.writableBytes() - 1; i >= startPos; --i) {
            slice.setByte(i, (byte) ('0' + (n % 10)));
            n /= 10;
        }
    }

}
