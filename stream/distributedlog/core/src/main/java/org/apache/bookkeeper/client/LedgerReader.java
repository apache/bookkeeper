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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException.BKNoSuchEntryException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.DistributionSchedule.WriteSet;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.checksum.DigestManager;

/**
 * Reader used for DL tools to read entries.
 */
public class LedgerReader {

    /**
     * Read Result Holder.
     */
    public static class ReadResult<T> {
        final long entryId;
        final int rc;
        final T value;
        final InetSocketAddress srcAddr;

        ReadResult(long entryId, int rc, T value, InetSocketAddress srcAddr) {
            this.entryId = entryId;
            this.rc = rc;
            this.value = value;
            this.srcAddr = srcAddr;
        }

        public long getEntryId() {
            return entryId;
        }

        public int getResultCode() {
            return rc;
        }

        public T getValue() {
            return value;
        }

        public InetSocketAddress getBookieAddress() {
            return srcAddr;
        }
    }

    private final ClientContext clientCtx;

    public LedgerReader(BookKeeper bkc) {
        clientCtx = bkc.getClientCtx();
    }

    public static SortedMap<Long, ? extends List<BookieSocketAddress>> bookiesForLedger(final LedgerHandle lh) {
        return lh.getLedgerMetadata().getAllEnsembles();
    }

    public void readEntriesFromAllBookies(final LedgerHandle lh, long eid,
                                          final GenericCallback<Set<ReadResult<ByteBuf>>> callback) {
        WriteSet writeSet = lh.distributionSchedule.getWriteSet(eid);
        final AtomicInteger numBookies = new AtomicInteger(writeSet.size());
        final Set<ReadResult<ByteBuf>> readResults = new HashSet<>();
        ReadEntryCallback readEntryCallback = new ReadEntryCallback() {
            @Override
            public void readEntryComplete(int rc, long lid, long eid, ByteBuf buffer, Object ctx) {
                BookieSocketAddress bookieAddress = (BookieSocketAddress) ctx;
                ReadResult<ByteBuf> rr;
                if (BKException.Code.OK != rc) {
                    rr = new ReadResult<>(eid, rc, null, bookieAddress.getSocketAddress());
                } else {
                    ByteBuf content;
                    try {
                        content = lh.macManager.verifyDigestAndReturnData(eid, buffer);
                        ByteBuf toRet = Unpooled.copiedBuffer(content);
                        rr = new ReadResult<>(eid, BKException.Code.OK, toRet, bookieAddress.getSocketAddress());
                    } catch (BKException.BKDigestMatchException e) {
                        rr = new ReadResult<>(
                            eid, BKException.Code.DigestMatchException, null, bookieAddress.getSocketAddress());

                    } finally {
                        buffer.release();
                    }
                }
                readResults.add(rr);
                if (numBookies.decrementAndGet() == 0) {
                    callback.operationComplete(BKException.Code.OK, readResults);
                }
            }
        };

        List<BookieSocketAddress> ensemble = lh.getLedgerMetadata().getEnsembleAt(eid);
        for (int i = 0; i < writeSet.size(); i++) {
            int idx = writeSet.get(i);
            clientCtx.getBookieClient().readEntry(ensemble.get(idx), lh.getId(), eid, readEntryCallback,
                                   ensemble.get(idx), BookieProtocol.FLAG_NONE);
        }
    }

    /**
     * Forward reading entries from last add confirmed.
     *
     * @param lh
     *          ledger handle to read entries
     * @param callback
     *          callback with the entries from last add confirmed.
     */
    public void forwardReadEntriesFromLastConfirmed(final LedgerHandle lh,
                                                    final GenericCallback<List<LedgerEntry>> callback) {
        final List<LedgerEntry> resultList = new ArrayList<LedgerEntry>();

        final FutureEventListener<LedgerEntries> readListener = new FutureEventListener<LedgerEntries>() {

            private void readNext(long entryId) {
                PendingReadOp op = new PendingReadOp(lh, clientCtx, entryId, entryId, false);
                op.future().whenComplete(this);
                op.submit();
            }

            @Override
            public void onSuccess(LedgerEntries ledgerEntries) {
                long entryId = -1L;
                for (org.apache.bookkeeper.client.api.LedgerEntry entry : ledgerEntries) {
                    resultList.add(new LedgerEntry((LedgerEntryImpl) entry));
                    entryId = entry.getEntryId();
                }
                try {
                    ledgerEntries.close();
                } catch (Exception e) {
                    // bk should not throw any exceptions here
                }
                ++entryId;
                readNext(entryId);
            }

            @Override
            public void onFailure(Throwable throwable) {
                if (throwable instanceof BKNoSuchEntryException) {
                    callback.operationComplete(Code.OK, resultList);
                } else {
                    int retCode;
                    if (throwable instanceof BKException) {
                        retCode = ((BKException) throwable).getCode();
                    } else {
                        retCode = Code.UnexpectedConditionException;
                    }
                    callback.operationComplete(retCode, resultList);
                }
            }
        };

        ReadLastConfirmedOp.LastConfirmedDataCallback readLACCallback = (rc, recoveryData) -> {
            if (BKException.Code.OK != rc) {
                callback.operationComplete(rc, resultList);
                return;
            }


            if (LedgerHandle.INVALID_ENTRY_ID >= recoveryData.getLastAddConfirmed()) {
                callback.operationComplete(BKException.Code.OK, resultList);
                return;
            }

            long entryId = recoveryData.getLastAddConfirmed();
            PendingReadOp op = new PendingReadOp(lh, clientCtx, entryId, entryId, false);
            op.future().whenComplete(readListener);
            op.submit();
        };
        // Read Last AddConfirmed
        new ReadLastConfirmedOp(clientCtx.getBookieClient(),
                                lh.distributionSchedule,
                                lh.macManager,
                                lh.ledgerId,
                                lh.getCurrentEnsemble(),
                                lh.ledgerKey,
                                readLACCallback).initiate();
    }

    public void readLacs(final LedgerHandle lh, long eid,
                         final GenericCallback<Set<ReadResult<Long>>> callback) {
        WriteSet writeSet = lh.distributionSchedule.getWriteSet(eid);
        final AtomicInteger numBookies = new AtomicInteger(writeSet.size());
        final Set<ReadResult<Long>> readResults = new HashSet<ReadResult<Long>>();
        ReadEntryCallback readEntryCallback = (rc, lid, eid1, buffer, ctx) -> {
            InetSocketAddress bookieAddress = (InetSocketAddress) ctx;
            ReadResult<Long> rr;
            if (BKException.Code.OK != rc) {
                rr = new ReadResult<Long>(eid1, rc, null, bookieAddress);
            } else {
                try {
                    DigestManager.RecoveryData data = lh.macManager.verifyDigestAndReturnLastConfirmed(buffer);
                    rr = new ReadResult<Long>(eid1, BKException.Code.OK, data.getLastAddConfirmed(), bookieAddress);
                } catch (BKException.BKDigestMatchException e) {
                    rr = new ReadResult<Long>(eid1, BKException.Code.DigestMatchException, null, bookieAddress);
                }
            }
            readResults.add(rr);
            if (numBookies.decrementAndGet() == 0) {
                callback.operationComplete(BKException.Code.OK, readResults);
            }
        };

        List<BookieSocketAddress> ensemble = lh.getLedgerMetadata().getEnsembleAt(eid);
        for (int i = 0; i < writeSet.size(); i++) {
            int idx = writeSet.get(i);
            clientCtx.getBookieClient().readEntry(ensemble.get(idx), lh.getId(), eid, readEntryCallback,
                                   ensemble.get(idx), BookieProtocol.FLAG_NONE);
        }
    }
}
