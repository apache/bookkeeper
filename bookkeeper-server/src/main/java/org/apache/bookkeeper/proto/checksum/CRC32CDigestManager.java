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
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CRC32CDigestManager extends DigestManager {
    static final Logger LOG = LoggerFactory.getLogger(CRC32CDigestManager.class);

    private final ThreadLocal<MutableInt> currentCrc = ThreadLocal
            .withInitial(() -> new MutableInt(0));
    private final ThreadLocal<MutableBoolean> isNewCrc = ThreadLocal
            .withInitial(() -> new MutableBoolean(true));

    public CRC32CDigestManager(long ledgerId) {
        super(ledgerId);
        if (!Sse42Crc32C.isSupported()) {
            LOG.error("Sse42Crc32C is not supported, will use less slower CRC32C implementation.");
        }
    }

    @Override
    int getMacCodeLength() {
        return 4;
    }

    @Override
    void populateValueAndReset(ByteBuf buf) {
        buf.writeInt(currentCrc.get().intValue());
        isNewCrc.get().setTrue();
    }

    @Override
    void update(ByteBuf data) {
        if (isNewCrc.get().isTrue()) {
            isNewCrc.get().setFalse();
            currentCrc.get().setValue(Crc32cIntChecksum.computeChecksum(data));
        } else {
            final int lastCrc = currentCrc.get().intValue();
            currentCrc.get().setValue(Crc32cIntChecksum.resumeChecksum(lastCrc, data));
        }
    }
}
