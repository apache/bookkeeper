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
package org.apache.bookkeeper.proto;

import java.util.Optional;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallbackCtx;

/**
 * A {@link ReadEntryCallbackCtx} for long poll read requests.
 */
public class ReadLastConfirmedAndEntryContext implements ReadEntryCallbackCtx {

    final int bookieIndex;
    final BookieSocketAddress bookie;
    long lac = LedgerHandle.INVALID_ENTRY_ID;
    Optional<Long> lacUpdateTimestamp = Optional.empty();

    public ReadLastConfirmedAndEntryContext(int bookieIndex, BookieSocketAddress bookie) {
        this.bookieIndex = bookieIndex;
        this.bookie = bookie;
    }

    public int getBookieIndex() {
        return bookieIndex;
    }

    public BookieSocketAddress getBookieAddress() {
        return bookie;
    }

    @Override
    public void setLastAddConfirmed(long lac) {
        this.lac = lac;
    }

    @Override
    public long getLastAddConfirmed() {
        return lac;
    }

    public Optional<Long> getLacUpdateTimestamp() {
        return lacUpdateTimestamp;
    }

    public void setLacUpdateTimestamp(long lacUpdateTimestamp) {
        this.lacUpdateTimestamp = Optional.of(lacUpdateTimestamp);
    }

}
