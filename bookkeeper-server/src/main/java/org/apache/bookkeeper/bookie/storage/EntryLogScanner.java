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
package org.apache.bookkeeper.bookie.storage;

import io.netty.buffer.ByteBuf;
import java.io.IOException;

/**
 * Scan entries in a entry log file.
 * Implementation for this interface should choose one of the following ReadLengthType:
 * READ_ALL, READ_NOTHING, READ_LEDGER_ENTRY_ID_LENGTH. <br/>
 * If the implementation chooses READ_ALL, it should implement {@link #process(long, long, ByteBuf)}. <br/>
 * If the implementation chooses READ_NOTHING, it should implement {@link #process(long, long, int)}. <br/>
 * If the implementation chooses READ_LEDGER_ENTRY_ID_LENGTH, it should implement {@link #process(long, long, int, long)}. <br/>
 *
 */
public interface EntryLogScanner {
    enum ReadLengthType {
        // Read all data of the entry
        READ_ALL(Integer.MAX_VALUE),
        // Read nothing of the entry
        READ_NOTHING(0),
        // Read ledger id(8 byte) and entry id(8 byte) in the beginning of the entry
        READ_LEDGER_ENTRY_ID(16);

        private final int lengthToRead;
        ReadLengthType(int lengthToRead) {
            this.lengthToRead = lengthToRead;
        }

        public int getLengthToRead() {
            return lengthToRead;
        }
    }

    /**
     * Tests whether or not the entries belongs to the specified ledger
     * should be processed.
     *
     * @param ledgerId ledger id
     * @return true if and only the entries of the ledger should be scanned.
     */
    boolean accept(long ledgerId);

    /**
     * Process an entry when ReadLengthType is READ_NOTHING.
     * @param ledgerId ledger id
     * @param offset init offset of the entry
     * @param entrySize entry size
     * @throws IOException
     */
    default void process(long ledgerId, long offset, int entrySize) throws IOException {
        throw new UnsupportedOperationException("Not implemented when ReadLengthType is READ_NOTHING");
    }

    /**
     * Process an entry when ReadLengthType is READ_LEDGER_ENTRY_ID_LENGTH.
     * @param ledgerId ledger id
     * @param offset init offset of the entry
     * @param entrySize entry size
     * @param entryId entry id
     * @throws IOException
     */
    default void process(long ledgerId, long offset, int entrySize, long entryId) throws IOException {
        throw new UnsupportedOperationException("Not implemented when ReadLengthType is READ_LEDGER_ENTRY_ID_LENGTH");
    }

    /**
     * Process an entry when ReadLengthType is READ_ALL.
     * @param ledgerId ledger id
     * @param offset init offset of the entry
     * @param entry entry, containing ledgerId(8byte), entryId(8byte),... entrySize=entry.readableBytes()
     */
    default void process(long ledgerId, long offset, ByteBuf entry) throws IOException{
        throw new UnsupportedOperationException("Not implemented when ReadLengthType is READ_ALL");
    }


    default ReadLengthType getReadLengthType(){
        return ReadLengthType.READ_ALL;
    }
}
