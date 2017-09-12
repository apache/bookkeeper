/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.client.api;

import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;

/**
 * Provide read access to a ledger
 */
public interface ReadHandler extends Handler {

    /**
     * Read a sequence of entries synchronously.
     *
     * @param firstEntry
     *          id of first entry of sequence (included)
     * @param lastEntry
     *          id of last entry of sequence (included)
     *
     * @see #asyncReadEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object)
     */
    public Enumeration<LedgerEntry> readEntries(long firstEntry, long lastEntry)
            throws InterruptedException, BKException;

    /**
     * Read a sequence of entries synchronously, allowing to read after the LastAddConfirmed range.<br>
     * This is the same of
     * {@link #asyncReadUnconfirmedEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object) }
     *
     * @param firstEntry
     *          id of first entry of sequence (included)
     * @param lastEntry
     *          id of last entry of sequence (included)
     *
     * @see #readEntries(long, long)
     * @see #asyncReadUnconfirmedEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object)
     * @see #asyncReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, java.lang.Object)
     */
    public Enumeration<LedgerEntry> readUnconfirmedEntries(long firstEntry, long lastEntry)
            throws InterruptedException, BKException;

    /**
     * Read a sequence of entries asynchronously.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence
     * @param cb
     *          object implementing read callback interface
     * @param ctx
     *          control object
     */
    public void asyncReadEntries(long firstEntry, long lastEntry, AsyncCallback.ReadCallback cb, Object ctx);

    /**
     * Read a sequence of entries asynchronously.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence
     */
    public CompletableFuture<Iterable<LedgerEntry>> asyncReadEntries(long firstEntry, long lastEntry);

    /**
     * Read a sequence of entries asynchronously, allowing to read after the LastAddConfirmed range.
     * <br>This is the same of
     * {@link #asyncReadEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object) }
     * but it lets the client read without checking the local value of LastAddConfirmed, so that it is possibile to
     * read entries for which the writer has not received the acknowledge yet. <br>
     * For entries which are within the range 0..LastAddConfirmed BookKeeper guarantees that the writer has successfully
     * received the acknowledge.<br>
     * For entries outside that range it is possible that the writer never received the acknowledge
     * and so there is the risk that the reader is seeing entries before the writer and this could result in a consistency
     * issue in some cases.<br>
     * With this method you can even read entries before the LastAddConfirmed and entries after it with one call,
     * the expected consistency will be as described above for each subrange of ids.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence
     * @param cb
     *          object implementing read callback interface
     * @param ctx
     *          control object
     *
     * @see #asyncReadEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object)
     * @see #asyncReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, java.lang.Object)
     * @see #readUnconfirmedEntries(long, long)
     */
    public void asyncReadUnconfirmedEntries(long firstEntry, long lastEntry, AsyncCallback.ReadCallback cb, Object ctx);

    /**
     * Read a sequence of entries asynchronously, allowing to read after the LastAddConfirmed range.
     * <br>This is the same of
     * {@link #asyncReadEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object) }
     * but it lets the client read without checking the local value of LastAddConfirmed, so that it is possibile to
     * read entries for which the writer has not received the acknowledge yet. <br>
     * For entries which are within the range 0..LastAddConfirmed BookKeeper guarantees that the writer has successfully
     * received the acknowledge.<br>
     * For entries outside that range it is possible that the writer never received the acknowledge
     * and so there is the risk that the reader is seeing entries before the writer and this could result in a consistency
     * issue in some cases.<br>
     * With this method you can even read entries before the LastAddConfirmed and entries after it with one call,
     * the expected consistency will be as described above for each subrange of ids.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence
     *
     * @see #asyncReadEntries(long, long, org.apache.bookkeeper.client.AsyncCallback.ReadCallback, java.lang.Object)
     * @see #asyncReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, java.lang.Object)
     * @see #readUnconfirmedEntries(long, long)
     */
    public CompletableFuture<Iterable<LedgerEntry>> asyncReadUnconfirmedEntries(long firstEntry, long lastEntry);

    /**
     * Obtains asynchronously the last confirmed write from a quorum of bookies. This
     * call obtains the the last add confirmed each bookie has received for this ledger
     * and returns the maximum. If the ledger has been closed, the value returned by this
     * call may not correspond to the id of the last entry of the ledger, since it reads
     * the hint of bookies. Consequently, in the case the ledger has been closed, it may
     * return a different value than getLastAddConfirmed, which returns the local value
     * of the ledger handle.
     *
     * @see #getLastAddConfirmed()
     *
     * @param cb
     * @param ctx
     */

    public void asyncReadLastConfirmed(final AsyncCallback.ReadLastConfirmedCallback cb, final Object ctx);

    /**
     * Obtains asynchronously the last confirmed write from a quorum of bookies.
     * It is similar as
     * {@link #asyncTryReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, Object)},
     * but it doesn't wait all the responses from the quorum. It would callback
     * immediately if it received a LAC which is larger than current LAC.
     *
     * @see #asyncTryReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, Object)
     *
     * @param cb
     *          callback to return read last confirmed
     * @param ctx
     *          callback context
     */
    public void asyncTryReadLastConfirmed(final AsyncCallback.ReadLastConfirmedCallback cb, final Object ctx);

    /**
     * Asynchronous read next entry and the latest last add confirmed.
     * If the next entryId is less than known last add confirmed, the call will read next entry directly.
     * If the next entryId is ahead of known last add confirmed, the call will issue a long poll read
     * to wait for the next entry <i>entryId</i>.
     *
     * The callback will return the latest last add confirmed and next entry if it is available within timeout period <i>timeOutInMillis</i>.
     *
     * @param entryId
     *          next entry id to read
     * @param timeOutInMillis
     *          timeout period to wait for the entry id to be available (for long poll only)
     * @param parallel
     *          whether to issue the long poll reads in parallel
     * @param cb
     *          callback to return the result
     * @param ctx
     *          callback context
     */
    public void asyncReadLastConfirmedAndEntry(final long entryId,
                                               final long timeOutInMillis,
                                               final boolean parallel,
                                               final AsyncCallback.ReadLastConfirmedAndEntryCallback cb,
                                               final Object ctx);

    /**
     * Obtains synchronously the last confirmed write from a quorum of bookies. This call
     * obtains the the last add confirmed each bookie has received for this ledger
     * and returns the maximum. If the ledger has been closed, the value returned by this
     * call may not correspond to the id of the last entry of the ledger, since it reads
     * the hint of bookies. Consequently, in the case the ledger has been closed, it may
     * return a different value than getLastAddConfirmed, which returns the local value
     * of the ledger handle.
     *
     * @see #getLastAddConfirmed()
     *
     * @return The entry id of the last confirmed write or {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID}
     *         if no entry has been confirmed
     * @throws InterruptedException
     * @throws BKException
     */
    public long readLastConfirmed()
            throws InterruptedException, BKException;

    /**
     * Obtains synchronously the last confirmed write from a quorum of bookies.
     * It is similar as {@link #readLastConfirmed()}, but it doesn't wait all the responses
     * from the quorum. It would callback immediately if it received a LAC which is larger
     * than current LAC.
     *
     * @see #readLastConfirmed()
     *
     * @return The entry id of the last confirmed write or {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID}
     *         if no entry has been confirmed
     * @throws InterruptedException
     * @throws BKException
     */
    public long tryReadLastConfirmed() throws InterruptedException, BKException;

    /**
     * Obtains asynchronously the explicit last add confirmed from a quorum of
     * bookies. This call obtains the the explicit last add confirmed each
     * bookie has received for this ledger and returns the maximum. If in the
     * write LedgerHandle, explicitLAC feature is not enabled then this will
     * return {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID}. If the read explicit
     * lastaddconfirmed is greater than getLastAddConfirmed, then it updates the
     * lastAddConfirmed of this ledgerhandle. If the ledger has been closed, it
     * returns the value of the last add confirmed from the metadata.
     *
     * @see #getLastAddConfirmed()
     *
     * @param cb
     *          callback to return read explicit last confirmed
     * @param ctx
     *          callback context
     */
    public void asyncReadExplicitLastConfirmed(final AsyncCallback.ReadLastConfirmedCallback cb, final Object ctx);

    /**
     * Obtains synchronously the explicit last add confirmed from a quorum of
     * bookies. This call obtains the the explicit last add confirmed each
     * bookie has received for this ledger and returns the maximum. If in the
     * write LedgerHandle, explicitLAC feature is not enabled then this will
     * return {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID}. If the read explicit
     * lastaddconfirmed is greater than getLastAddConfirmed, then it updates the
     * lastAddConfirmed of this ledgerhandle. If the ledger has been closed, it
     * returns the value of the last add confirmed from the metadata.
     *
     * @see #getLastAddConfirmed()
     *
     * @return The entry id of the explicit last confirmed write or
     *         {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID} if no entry has been
     *         confirmed or if explicitLAC feature is not enabled in write
     *         LedgerHandle.
     * @throws InterruptedException
     * @throws BKException
     */
    public long readExplicitLastConfirmed() throws InterruptedException, BKException;

    /**
     * Get the local value for LastAddConfirmed
     * @return the local value for LastAddConfirmed
     */
    long getLastAddConfirmed();

}
