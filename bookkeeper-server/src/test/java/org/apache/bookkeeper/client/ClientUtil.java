/**
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
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ClientUtil {
    public static ByteBuf generatePacket(long ledgerId, long entryId, long lastAddConfirmed,
                                               long length, byte[] data) {
        return generatePacket(ledgerId, entryId, lastAddConfirmed, length, data, 0, data.length);
    }

    public static ByteBuf generatePacket(long ledgerId, long entryId, long lastAddConfirmed,
                                               long length, byte[] data, int offset, int len) {
        CRC32DigestManager dm = new CRC32DigestManager(ledgerId);
        return dm.computeDigestAndPackageForSending(entryId, lastAddConfirmed, length,
                                                    Unpooled.wrappedBuffer(data, offset, len));
    }

    /** Returns that whether ledger is in open state */
    public static boolean isLedgerOpen(LedgerHandle handle) {
        return !handle.metadata.isClosed();
    }

}
