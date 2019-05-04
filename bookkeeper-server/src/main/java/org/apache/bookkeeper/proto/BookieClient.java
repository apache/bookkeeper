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
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ForceLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.ByteBufList;

/**
 * Low level client for talking to bookies.
 */
public interface BookieClient {
    long PENDINGREQ_NOTWRITABLE_MASK = 0x01L << 62;

    /**
     * Get the list of bookies which have exhibited more error responses
     * than a configured threshold.
     *
     * @return the list of faulty bookies
     */
    List<BookieSocketAddress> getFaultyBookies();

    /**
     * Check whether the channel used to write to a bookie channel is writable.
     * A channel becomes non-writable when its buffer become full, and will stay
     * non-writable until some of the buffer is cleared.
     *
     * <p>This can be used to apply backpressure. If a channel is not writable,
     * requests will end up queuing.
     *
     * <p>As as we use pooling, there may be multiple channels per bookie, so
     * we also pass the ledger ID to check the writability of the correct
     * channel.
     *
     * <p>This has nothing to do with the bookie read-only status.
     *
     * @param address the address of the bookie
     * @param ledgerId the ledger we wish to send a request to
     *
     */
    boolean isWritable(BookieSocketAddress address, long ledgerId);

    /**
     * Get the number of outstanding requests on the channel used to connect
     * to a bookie at {@code address} for a ledger with {@code ledgerId}.
     * It is necessary to specify the ledgerId as there may be multiple
     * channels for a single bookie if pooling is in use.
     * If the bookie is not {@link #isWritable(BookieSocketAddress,long) writable},
     * then the {@link #PENDINGREQ_NOTWRITABLE_MASK} will be logically or'd with
     * the returned value.
     *
     * @param address the address of the bookie
     * @param ledgerId the ledger whose channel we wish to query
     * @return the number of requests currently outstanding
     */
    long getNumPendingRequests(BookieSocketAddress address, long ledgerId);

    /**
     * Send a force request to the server. When complete all entries which have
     * been written for {@code ledgerId} to this bookie will be persisted on disk.
     * This is for use with {@link org.apache.bookkeeper.client.api.WriteFlag#DEFERRED_SYNC}.
     *
     * @param address the address of the bookie
     * @param ledgerId the ledger whose entries we want persisted
     * @param cb the callback notified when the request completes
     * @param ctx a context object passed to the callback on completion
     */
    void forceLedger(BookieSocketAddress address, long ledgerId,
                     ForceLedgerCallback cb, Object ctx);

    /**
     * Read the last add confirmed for ledger {@code ledgerId} from the bookie at
     * {@code address}.
     *
     * @param address the address of the bookie
     * @param ledgerId the ledger whose last add confirm we wish to know
     * @param cb the callback notified when the request completes
     * @param ctx a context object passed to the callback on completion
     */
    void readLac(BookieSocketAddress address, long ledgerId, ReadLacCallback cb, Object ctx);

    /**
     * Explicitly write the last add confirmed for ledger {@code ledgerId} to the bookie at
     * {@code address}.
     *
     * @param address the address of the bookie
     * @param ledgerId the ledger whose last add confirm we wish to know
     * @param masterKey the master key of the ledger
     * @param lac the last add confirmed we wish to write
     * @param toSend a buffer also containing the lac, along with a digest
     * @param cb the callback notified when the request completes
     * @param ctx a context object passed to the callback on completion
     */
    void writeLac(BookieSocketAddress address, long ledgerId, byte[] masterKey,
                  long lac, ByteBufList toSend, WriteLacCallback cb, Object ctx);

    /**
     * Add an entry for ledger {@code ledgerId} on the bookie at address {@code address}.
     *
     * @param address the address of the bookie
     * @param ledgerId the ledger to which we wish to add the entry
     * @param entryId the id of the entry we wish to add
     * @param toSend a buffer containing the entry and its digest
     * @param cb the callback notified when the request completes
     * @param ctx a context object passed to the callback on completion
     * @param options a bit mask of flags from BookieProtocol.FLAG_*
     *                {@link org.apache.bookkeeper.proto.BookieProtocol}
     * @param allowFastFail fail the add immediately if the channel is non-writable
     *                      {@link #isWritable(BookieSocketAddress,long)}
     * @param writeFlags a set of write flags
     *                   {@link org.apache.bookkeeper.client.api.WriteFlags}
     */
    void addEntry(BookieSocketAddress address, long ledgerId, byte[] masterKey,
                  long entryId, ByteBufList toSend, WriteCallback cb, Object ctx,
                  int options, boolean allowFastFail, EnumSet<WriteFlag> writeFlags);

    /**
     * Read entry with a null masterkey, disallowing failfast.
     * @see #readEntry(BookieSocketAddress,long,long,ReadEntryCallback,Object,int,byte[],boolean)
     */
    default void readEntry(BookieSocketAddress address, long ledgerId, long entryId,
                           ReadEntryCallback cb, Object ctx, int flags) {
        readEntry(address, ledgerId, entryId, cb, ctx, flags, null);
    }

    /**
     * Read entry, disallowing failfast.
     * @see #readEntry(BookieSocketAddress,long,long,ReadEntryCallback,Object,int,byte[],boolean)
     */
    default void readEntry(BookieSocketAddress address, long ledgerId, long entryId,
                           ReadEntryCallback cb, Object ctx, int flags, byte[] masterKey) {
        readEntry(address, ledgerId, entryId, cb, ctx, flags, masterKey, false);
    }

    /**
     * Read an entry from bookie at address {@code address}.
     *
     * @param address address of the bookie to read from
     * @param ledgerId id of the ledger the entry belongs to
     * @param entryId id of the entry we wish to read
     * @param cb the callback notified when the request completes
     * @param ctx a context object passed to the callback on completion
     * @param flags a bit mask of flags from BookieProtocol.FLAG_*
     *              {@link org.apache.bookkeeper.proto.BookieProtocol}
     * @param masterKey the master key of the ledger being read from. This is only required
     *                  if the FLAG_DO_FENCING is specified.
     * @param allowFastFail fail the read immediately if the channel is non-writable
     *                      {@link #isWritable(BookieSocketAddress,long)}
     */
    void readEntry(BookieSocketAddress address, long ledgerId, long entryId,
                   ReadEntryCallback cb, Object ctx, int flags, byte[] masterKey,
                   boolean allowFastFail);

    /**
     * Send a long poll request to bookie, waiting for the last add confirmed
     * to be updated. The client can also request that the full entry is returned
     * with the new last add confirmed.
     *
     * @param address address of bookie to send the long poll address to
     * @param ledgerId ledger whose last add confirmed we are interested in
     * @param entryId the id of the entry we expect to read
     * @param previousLAC the previous lac value
     * @param timeOutInMillis number of millis to wait for LAC update
     * @param piggyBackEntry whether to read the requested entry when LAC is updated
     * @param cb the callback notified when the request completes
     * @param ctx a context object passed to the callback on completion
     */
    void readEntryWaitForLACUpdate(BookieSocketAddress address,
                                   long ledgerId,
                                   long entryId,
                                   long previousLAC,
                                   long timeOutInMillis,
                                   boolean piggyBackEntry,
                                   ReadEntryCallback cb,
                                   Object ctx);

    /**
     * Read information about the bookie, from the bookie.
     *
     * @param address the address of the bookie to request information from
     * @param requested a bitset specifying which pieces of information to request
     *                  {@link org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoRequest}
     * @param cb the callback notified when the request completes
     * @param ctx a context object passed to the callback on completion
     *
     * @see org.apache.bookkeeper.client.BookieInfoReader.BookieInfo
     */
    void getBookieInfo(BookieSocketAddress address, long requested,
                       GetBookieInfoCallback cb, Object ctx);

    /**
     * Makes async request for getting list of entries of ledger from a bookie
     * and returns Future for the result.
     *
     * @param address
     *            BookieSocketAddress of the bookie
     * @param ledgerId
     *            ledgerId
     * @return returns Future
     */
    CompletableFuture<AvailabilityOfEntriesOfLedger> getListOfEntriesOfLedger(BookieSocketAddress address,
            long ledgerId);

    /**
     * @return whether bookie client object has been closed
     */
    boolean isClosed();

    /**
     * Close the bookie client object.
     */
    void close();
}
