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
package com.scurrilous.circe.checksum;

import com.scurrilous.circe.crc.Sse42Crc32C;
import io.netty.buffer.ByteBuf;

public class Crc32cIntChecksum {

    private final static IntHash CRC32C_HASH;

    static {
        if (Sse42Crc32C.isSupported()) {
            CRC32C_HASH = new JniIntHash();
        } else if (Java9IntHash.HAS_JAVA9_CRC32C) {
            CRC32C_HASH = new Java9IntHash();
        } else {
            CRC32C_HASH = new Java8IntHash();
        }
    }

    /**
     * Computes crc32c checksum: if it is able to load crc32c native library then it computes using that native library
     * which is faster as it computes using hardware machine instruction else it computes using crc32c algo.
     *
     * @param payload
     * @return
     */
    public static int computeChecksum(ByteBuf payload) {
        return CRC32C_HASH.calculate(payload);
    }

    /**
     * Computes crc32c checksum: if it is able to load crc32c native library then it computes using that native library
     * which is faster as it computes using hardware machine instruction else it computes using crc32c algo.
     *
     * @param payload
     * @return
     */
    public static int computeChecksum(ByteBuf payload, int offset, int len) {
        return CRC32C_HASH.calculate(payload, offset, len);
    }

    /**
     * Computes incremental checksum with input previousChecksum and input payload
     *
     * @param previousChecksum : previously computed checksum
     * @param payload
     * @return
     */
    public static int resumeChecksum(int previousChecksum, ByteBuf payload) {
        return CRC32C_HASH.resume(previousChecksum, payload);
    }

    /**
     * Computes incremental checksum with input previousChecksum and input payload
     *
     * @param previousChecksum : previously computed checksum
     * @param payload
     * @return
     */
    public static int resumeChecksum(int previousChecksum, ByteBuf payload, int offset, int len) {
        return CRC32C_HASH.resume(previousChecksum, payload, offset, len);
    }

}
