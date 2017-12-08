/**
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
package org.apache.bookkeeper.client.api;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;

/**
 * Provide read access to a ledger.
 *
 * @since 4.6
 */
@Public
@Unstable
public interface ReadHandle extends Handle {

    /**
     * Read a sequence of entries asynchronously.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence, inclusive
     * @return an handle to the result of the operation
     */
    CompletableFuture<Iterable<LedgerEntry>> read(long firstEntry, long lastEntry);

    /**
     * Read a sequence of entries asynchronously, allowing to read after the LastAddConfirmed range.
     * <br>This is the same of
     * {@link #read(long, long) }
     * but it lets the client read without checking the local value of LastAddConfirmed, so that it is possibile to
     * read entries for which the writer has not received the acknowledge yet. <br>
     * For entries which are within the range 0..LastAddConfirmed BookKeeper guarantees that the writer has successfully
     * received the acknowledge.<br>
     * For entries outside that range it is possible that the writer never received the acknowledge
     * and so there is the risk that the reader is seeing entries before the writer and this could
     * result in a consistency issue in some cases.<br>
     * With this method you can even read entries before the LastAddConfirmed and entries after it with one call,
     * the expected consistency will be as described above for each subrange of ids.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence, inclusive
     * @return an handle to the result of the operation
     *
     * @see #read(long, long)
     * @see #readLastAddConfirmed()
     */
    CompletableFuture<Iterable<LedgerEntry>> readUnconfirmed(long firstEntry, long lastEntry);

    /**
     * Obtains asynchronously the last confirmed write from a quorum of bookies. This
     * call obtains the the last add confirmed each bookie has received for this ledger
     * and returns the maximum. If the ledger has been closed, the value returned by this
     * call may not correspond to the id of the last entry of the ledger, since it reads
     * the hint of bookies. Consequently, in the case the ledger has been closed, it may
     * return a different value than getLastAddConfirmed, which returns the local value
     * of the ledger handle.
     *
     * @return an handle to the result of the operation
     * @see #getLastAddConfirmed()
     *
     */
    CompletableFuture<Long> readLastAddConfirmed();

    /**
     * Obtains asynchronously the last confirmed write from a quorum of bookies
     * but it doesn't wait all the responses from the quorum. It would callback
     * immediately if it received a LAC which is larger than current LAC.
     *
     * @return an handle to the result of the operation
     * @see #tryReadLastAddConfirmed()
     *
     */
    CompletableFuture<Long> tryReadLastAddConfirmed();

    /**
     * Get the last confirmed entry id on this ledger. It reads the local state of the ledger handle,
     * which is different from the {@link #readLastAddConfirmed()} call.

     * <p>In the case the ledger is not closed and the client is a reader, it is necessary to
     * call {@link #readLastAddConfirmed()} to obtain a fresh value of last add confirmed entry id.
     *
     * @see #readLastAddConfirmed()
     *
     * @return the local value for LastAddConfirmed or -1L if no entry has been confirmed.
     */
    long getLastAddConfirmed();

    /**
     * Returns the length of the data written in this ledger so much, in bytes.
     *
     * @return the length of the data written in this ledger, in bytes.
     */
    long getLength();

    /**
     * Asynchronous read specific entry and the latest last add confirmed.
     * If the next entryId is less than known last add confirmed, the call will read next entry directly.
     * If the next entryId is ahead of known last add confirmed, the call will issue a long poll read
     * to wait for the next entry <i>entryId</i>.
     *
     * @param entryId
     *          next entry id to read
     * @param timeOutInMillis
     *          timeout period to wait for the entry id to be available (for long poll only)
     *          if timeout for get the entry, it will return null entry.
     * @param parallel
     *          whether to issue the long poll reads in parallel
     * @return an handle to the result of the operation
     */
    CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntry(long entryId,
                                                                          long timeOutInMillis,
                                                                          boolean parallel);

}
