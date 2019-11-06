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

import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import com.scurrilous.circe.crc.Sse42Crc32C;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocal;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.mutable.MutableInt;

@Slf4j
class CRC32CDigestManager extends DigestManager {

    private static boolean nonSupportedMessagePrinted = false;

    private static final FastThreadLocal<MutableInt> currentCrc = new FastThreadLocal<MutableInt>() {
        @Override
        protected MutableInt initialValue() throws Exception {
            return new MutableInt(0);
        }
    };

    public CRC32CDigestManager(long ledgerId, boolean useV2Protocol, ByteBufAllocator allocator) {
        super(ledgerId, useV2Protocol, allocator);

        if (!Sse42Crc32C.isSupported() && !nonSupportedMessagePrinted) {
            log.warn("Sse42Crc32C is not supported, will use a slower CRC32C implementation.");
            nonSupportedMessagePrinted = true;
        }
    }

    @Override
    int getMacCodeLength() {
        return 4;
    }

    @Override
    void populateValueAndReset(ByteBuf buf) {
        MutableInt current = currentCrc.get();
        buf.writeInt(current.intValue());
        current.setValue(0);
    }

    @Override
    void update(ByteBuf data) {
        MutableInt current = currentCrc.get();
        final int lastCrc = current.intValue();
        current.setValue(Crc32cIntChecksum.resumeChecksum(lastCrc, data));
    }
}
