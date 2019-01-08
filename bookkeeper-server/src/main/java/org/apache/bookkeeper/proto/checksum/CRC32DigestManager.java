package org.apache.bookkeeper.proto.checksum;

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocal;

/**
 * Digest manager for CRC32 checksum.
 */
class CRC32DigestManager extends DigestManager {

    /**
     * Interface that abstracts different implementations of the CRC32 digest.
     */
    interface CRC32Digest {
        long getValueAndReset();

        void update(ByteBuf buf);
    }

    private static final FastThreadLocal<CRC32Digest> crc = new FastThreadLocal<CRC32Digest>() {
        @Override
        protected CRC32Digest initialValue() {
            if (DirectMemoryCRC32Digest.isSupported()) {
                return new DirectMemoryCRC32Digest();
            } else {
                return new StandardCRC32Digest();
            }
        }
    };

    public CRC32DigestManager(long ledgerId, boolean useV2Protocol, ByteBufAllocator allocator) {
        super(ledgerId, useV2Protocol, allocator);
    }

    @Override
    int getMacCodeLength() {
        return 8;
    }

    @Override
    void populateValueAndReset(ByteBuf buf) {
        buf.writeLong(crc.get().getValueAndReset());
    }

    @Override
    void update(ByteBuf data) {
        crc.get().update(data);
    }
}
