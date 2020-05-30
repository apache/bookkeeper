/*
 *
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
 *
 */

package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.tools.cli.commands.bookie.FormatUtil.bytes2Hex;

import io.netty.buffer.ByteBuf;
import java.io.Closeable;
import java.io.IOException;
import java.util.PrimitiveIterator.OfLong;

import org.apache.bookkeeper.common.util.Watcher;

/**
 * This class maps a ledger entry number into a location (entrylogid, offset) in
 * an entry log file. It does user level caching to more efficiently manage disk
 * head scheduling.
 */
public interface LedgerCache extends Closeable {

    boolean setFenced(long ledgerId) throws IOException;
    boolean isFenced(long ledgerId) throws IOException;

    void setMasterKey(long ledgerId, byte[] masterKey) throws IOException;
    byte[] readMasterKey(long ledgerId) throws IOException, BookieException;
    boolean ledgerExists(long ledgerId) throws IOException;

    void putEntryOffset(long ledger, long entry, long offset) throws IOException;
    long getEntryOffset(long ledger, long entry) throws IOException;

    void flushLedger(boolean doAll) throws IOException;
    long getLastEntry(long ledgerId) throws IOException;

    Long getLastAddConfirmed(long ledgerId) throws IOException;
    long updateLastAddConfirmed(long ledgerId, long lac) throws IOException;
    boolean waitForLastAddConfirmedUpdate(long ledgerId,
                                          long previousLAC,
                                          Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException;
    void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                             Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException;

    void deleteLedger(long ledgerId) throws IOException;

    void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException;
    ByteBuf getExplicitLac(long ledgerId);

    /**
     * Specific exception to encode the case where the index is not present.
     */
    class NoIndexForLedgerException extends IOException {
        NoIndexForLedgerException(String reason, Exception cause) {
            super(reason, cause);
        }
    }

    /**
     * Represents a page of the index.
     */
    interface PageEntries {
        LedgerEntryPage getLEP() throws IOException;
        long getFirstEntry();
        long getLastEntry();
    }

    /**
     * Iterable over index pages -- returns PageEntries rather than individual
     * entries because getEntries() above needs to be able to throw an IOException.
     */
    interface PageEntriesIterable extends AutoCloseable, Iterable<PageEntries> {}

    PageEntriesIterable listEntries(long ledgerId) throws IOException;

    OfLong getEntriesIterator(long ledgerId) throws IOException;

    /**
     * Represents summary of ledger metadata.
     */
    class LedgerIndexMetadata {
        public final byte[] masterKey;
        public final long size;
        public final boolean fenced;
        LedgerIndexMetadata(byte[] masterKey, long size, boolean fenced) {
            this.masterKey = masterKey;
            this.size = size;
            this.fenced = fenced;
        }

        public String getMasterKeyHex() {
            if (null == masterKey) {
                return "NULL";
            } else {
                return bytes2Hex(masterKey);
            }
        }
    }

    LedgerIndexMetadata readLedgerIndexMetadata(long ledgerId) throws IOException;
}
