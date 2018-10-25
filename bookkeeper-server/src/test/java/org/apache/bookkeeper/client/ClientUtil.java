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

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.security.GeneralSecurityException;
import java.util.function.Function;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallbackFuture;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.util.ByteBufList;

/**
 * Client utilities.
 */
public class ClientUtil {
    public static final byte[] PASSWD = "foobar".getBytes(UTF_8);

    public static ByteBuf generatePacket(long ledgerId, long entryId, long lastAddConfirmed,
                                               long length, byte[] data) throws GeneralSecurityException {
        return generatePacket(ledgerId, entryId, lastAddConfirmed, length, data, 0, data.length);
    }

    public static ByteBuf generatePacket(long ledgerId, long entryId, long lastAddConfirmed, long length, byte[] data,
            int offset, int len) throws GeneralSecurityException {
        DigestManager dm = DigestManager.instantiate(ledgerId, new byte[2], DigestType.CRC32);
        return ByteBufList.coalesce(dm.computeDigestAndPackageForSending(entryId, lastAddConfirmed, length,
                Unpooled.wrappedBuffer(data, offset, len)));
    }

    /**
     * Returns that whether ledger is in open state.
     */
    public static boolean isLedgerOpen(LedgerHandle handle) {
        return !handle.getLedgerMetadata().isClosed();
    }

    public static LedgerMetadata setupLedger(ClientContext clientCtx, long ledgerId,
                                             LedgerMetadataBuilder builder) throws Exception {
        LedgerMetadata md = builder.withPassword(PASSWD).build();
        GenericCallbackFuture<LedgerMetadata> mdPromise = new GenericCallbackFuture<>();
        clientCtx.getLedgerManager().createLedgerMetadata(ledgerId, md, mdPromise);
        return mdPromise.get();
    }

    public static LedgerMetadata transformMetadata(ClientContext clientCtx, long ledgerId,
                                                   Function<LedgerMetadata, LedgerMetadata> transform)
            throws Exception {
        GenericCallbackFuture<LedgerMetadata> readPromise = new GenericCallbackFuture<>();
        GenericCallbackFuture<LedgerMetadata> writePromise = new GenericCallbackFuture<>();
        clientCtx.getLedgerManager().readLedgerMetadata(ledgerId, readPromise);
        clientCtx.getLedgerManager().writeLedgerMetadata(ledgerId, transform.apply(readPromise.get()), writePromise);
        return writePromise.get();
    }
}
