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
package org.apache.bookkeeper.metadata.etcd;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Utils for etcd based metadata store.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class EtcdUtils {

    private static final String LEDGER_IDGEN_PREFIX = "IDGEN";
    private static final int DEFAULT_IDGEN_BUCKET = 0;

    static String getIdGenKey(String scope) {
        // TODO: add a bucket id here to allow improving this to a concurrent
        //       id generation approach.
        return String.format("%s/%s/%06d",
            scope,
            LEDGER_IDGEN_PREFIX,
            DEFAULT_IDGEN_BUCKET);
    }

    public static long toLong(byte[] memory, int index) {
        return ((long) memory[index] & 0xff) << 56
            | ((long) memory[index + 1] & 0xff) << 48
            | ((long) memory[index + 2] & 0xff) << 40
            | ((long) memory[index + 3] & 0xff) << 32
            | ((long) memory[index + 4] & 0xff) << 24
            | ((long) memory[index + 5] & 0xff) << 16
            | ((long) memory[index + 6] & 0xff) << 8
            | (long) memory[index + 7] & 0xff;
    }

    /**
     * Convert a long number to a bytes array.
     *
     * @param value the long number
     * @return the bytes array
     */
    public static byte[] toBytes(long value) {
        byte[] memory = new byte[8];
        toBytes(value, memory, 0);
        return memory;
    }

    public static void toBytes(long value, byte[] memory, int index) {
        memory[index] = (byte) (value >>> 56);
        memory[index + 1] = (byte) (value >>> 48);
        memory[index + 2] = (byte) (value >>> 40);
        memory[index + 3] = (byte) (value >>> 32);
        memory[index + 4] = (byte) (value >>> 24);
        memory[index + 5] = (byte) (value >>> 16);
        memory[index + 6] = (byte) (value >>> 8);
        memory[index + 7] = (byte) value;
    }

}
