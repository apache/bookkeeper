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
package org.apache.bookkeeper.proto;

import java.util.EnumSet;
import java.util.List;

import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ForceLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.util.ByteBufList;

/**
 * Low level client for talking to bookies.
 */
public interface BookieClient {
    List<BookieSocketAddress> getFaultyBookies();

    boolean isWritable(BookieSocketAddress address, long key);
    PerChannelBookieClientPool lookupClient(BookieSocketAddress addr);
    void forceLedger(BookieSocketAddress addr, long ledgerId,
                     ForceLedgerCallback cb, Object ctx);
    void writeLac(BookieSocketAddress addr, long ledgerId, byte[] masterKey,
                  long lac, ByteBufList toSend, WriteLacCallback cb, Object ctx);
    void addEntry(BookieSocketAddress addr, long ledgerId, byte[] masterKey,
                  long entryId, ByteBufList toSend, WriteCallback cb, Object ctx,
                  int options, boolean allowFastFail, EnumSet<WriteFlag> writeFlags);
    void readLac(BookieSocketAddress addr, long ledgerId, ReadLacCallback cb, Object ctx);
    default void readEntry(BookieSocketAddress addr, long ledgerId, long entryId,
                   ReadEntryCallback cb, Object ctx, int flags) {
        readEntry(addr, ledgerId, entryId, cb, ctx, flags, null);
    }

    default void readEntry(BookieSocketAddress addr, long ledgerId, long entryId,
                           ReadEntryCallback cb, Object ctx, int flags, byte[] masterKey) {
        readEntry(addr, ledgerId, entryId, cb, ctx, flags, masterKey, false);
    }

    void readEntry(BookieSocketAddress addr, long ledgerId, long entryId,
                   ReadEntryCallback cb, Object ctx, int flags, byte[] masterKey,
                   boolean allowFastFail);

    void readEntryWaitForLACUpdate(BookieSocketAddress addr,
                                   long ledgerId,
                                   long entryId,
                                   long previousLAC,
                                   long timeOutInMillis,
                                   boolean piggyBackEntry,
                                   ReadEntryCallback cb,
                                   Object ctx);
    void getBookieInfo(BookieSocketAddress addr, long requested,
                       GetBookieInfoCallback cb, Object ctx);

    boolean isClosed();
    void close();
}
