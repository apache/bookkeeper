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

import static org.apache.bookkeeper.proto.BookieProtocol.FLAG_RECOVERY_ADD;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ForceLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.FutureGetListOfEntriesOfLedger;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.ByteBufList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mock implementation of BookieClient.
 */
public class MockBookieClient implements BookieClient {
    static final Logger LOG = LoggerFactory.getLogger(MockBookieClient.class);

    final OrderedExecutor executor;
    final MockBookies mockBookies;
    final Set<BookieId> errorBookies =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Runs before or after an operation. Can stall the operation or error it.
     */
    public interface Hook {
        CompletableFuture<Void> runHook(BookieId bookie, long ledgerId, long entryId);
    }

    private Hook preReadHook = (bookie, ledgerId, entryId) -> FutureUtils.value(null);
    private Hook postReadHook = (bookie, ledgerId, entryId) -> FutureUtils.value(null);
    private Hook preWriteHook = (bookie, ledgerId, entryId) -> FutureUtils.value(null);
    private Hook postWriteHook = (bookie, ledgerId, entryId) -> FutureUtils.value(null);

    public MockBookieClient(OrderedExecutor executor) {
        this.executor = executor;
        this.mockBookies = new MockBookies();
    }

    public MockBookieClient(OrderedExecutor executor,
                            MockBookies mockBookies) {
        this.executor = executor;
        this.mockBookies = mockBookies;
    }

    public void setPreReadHook(Hook hook) {
        this.preReadHook = hook;
    }

    public void setPostReadHook(Hook hook) {
        this.postReadHook = hook;
    }

    public void setPreWriteHook(Hook hook) {
        this.preWriteHook = hook;
    }

    public void setPostWriteHook(Hook hook) {
        this.postWriteHook = hook;
    }

    public void errorBookies(BookieId... bookies) {
        errorBookies.addAll(Arrays.asList(bookies));
    }

    public void removeErrors(BookieId... bookies) {
        for (BookieId b : bookies) {
            errorBookies.remove(b);
        }
    }

    public boolean isErrored(BookieId bookieId) {
        return errorBookies.contains(bookieId);
    }

    public MockBookies getMockBookies() {
        return mockBookies;
    }

    @Override
    public List<BookieId> getFaultyBookies() {
        return Collections.emptyList();
    }

    @Override
    public boolean isWritable(BookieId address, long ledgerId) {
        return true;
    }

    @Override
    public long getNumPendingRequests(BookieId address, long ledgerId) {
        return 0;
    }

    @Override
    public void forceLedger(BookieId addr, long ledgerId,
                            ForceLedgerCallback cb, Object ctx) {
        executor.executeOrdered(ledgerId,
                () -> cb.forceLedgerComplete(BKException.Code.IllegalOpException, ledgerId, addr, ctx));
    }

    @Override
    public void writeLac(BookieId addr, long ledgerId, byte[] masterKey,
                         long lac, ByteBufList toSend, WriteLacCallback cb, Object ctx) {
        executor.executeOrdered(ledgerId,
                () -> cb.writeLacComplete(BKException.Code.IllegalOpException, ledgerId, addr, ctx));
    }

    @Override
    public void addEntry(BookieId addr, long ledgerId, byte[] masterKey,
                         long entryId, ReferenceCounted toSend, WriteCallback cb, Object ctx,
                         int options, boolean allowFastFail, EnumSet<WriteFlag> writeFlags) {
        toSend.retain();
        preWriteHook.runHook(addr, ledgerId, entryId)
                .thenComposeAsync(
                        (ignore) -> {
                            LOG.info("[{};L{}] write entry {}", addr, ledgerId, entryId);
                            if (isErrored(addr)) {
                                LOG.warn("[{};L{}] erroring write {}", addr, ledgerId, entryId);
                                return FutureUtils.exception(new BKException.BKWriteException());
                            }

                            try {
                                if ((options & FLAG_RECOVERY_ADD) == FLAG_RECOVERY_ADD) {
                                    mockBookies.recoveryAddEntry(addr, ledgerId, entryId, copyData(toSend));
                                } else {
                                    mockBookies.addEntry(addr, ledgerId, entryId, copyData(toSend));
                                }
                            } catch (BKException bke) {
                                return FutureUtils.exception(bke);
                            } finally {
                                toSend.release();
                            }

                            return FutureUtils.value(null);
                        }, executor.chooseThread(ledgerId))
                .thenCompose((res) -> postWriteHook.runHook(addr, ledgerId, entryId))
                .whenCompleteAsync((res, ex) -> {
                    if (ex != null) {
                        cb.writeComplete(BKException.getExceptionCode(ex, BKException.Code.WriteException),
                                ledgerId, entryId, addr, ctx);
                    } else {
                        cb.writeComplete(BKException.Code.OK, ledgerId, entryId, addr, ctx);
                    }
                }, executor.chooseThread(ledgerId));
    }

    @Override
    public void readLac(BookieId addr, long ledgerId, ReadLacCallback cb, Object ctx) {
        executor.executeOrdered(ledgerId,
                () -> cb.readLacComplete(BKException.Code.IllegalOpException, ledgerId, null, null, ctx));
    }

    @Override
    public void readEntry(BookieId addr, long ledgerId, long entryId,
                          ReadEntryCallback cb, Object ctx, int flags, byte[] masterKey,
                          boolean allowFastFail) {
        preReadHook.runHook(addr, ledgerId, entryId)
                .thenComposeAsync((res) -> {
                    LOG.info("[{};L{}] read entry {}", addr, ledgerId, entryId);
                    if (isErrored(addr)) {
                        LOG.warn("[{};L{}] erroring read {}", addr, ledgerId, entryId);
                        return FutureUtils.exception(new BKException.BKReadException());
                    }

                    try {
                        ByteBuf entry = mockBookies.readEntry(addr, flags, ledgerId, entryId);
                        return FutureUtils.value(entry);
                    } catch (BKException bke) {
                        return FutureUtils.exception(bke);
                    }
                }, executor.chooseThread(ledgerId))
                .thenCompose((buf) -> postReadHook.runHook(addr, ledgerId, entryId).thenApply((res) -> buf))
                .whenCompleteAsync((res, ex) -> {
                    if (ex != null) {
                        cb.readEntryComplete(BKException.getExceptionCode(ex, BKException.Code.ReadException),
                                ledgerId, entryId, null, ctx);
                    } else {
                        cb.readEntryComplete(BKException.Code.OK,
                                ledgerId, entryId, res.slice(), ctx);
                    }
                }, executor.chooseThread(ledgerId));
    }

    @Override
    public void readEntryWaitForLACUpdate(BookieId addr,
                                          long ledgerId,
                                          long entryId,
                                          long previousLAC,
                                          long timeOutInMillis,
                                          boolean piggyBackEntry,
                                          ReadEntryCallback cb,
                                          Object ctx) {
        executor.executeOrdered(ledgerId,
                () -> cb.readEntryComplete(BKException.Code.IllegalOpException, ledgerId, entryId, null, ctx));
    }

    @Override
    public void getBookieInfo(BookieId addr, long requested,
                              GetBookieInfoCallback cb, Object ctx) {
        executor.executeOrdered(addr,
                () -> cb.getBookieInfoComplete(BKException.Code.IllegalOpException, null, ctx));
    }

    @Override
    public CompletableFuture<AvailabilityOfEntriesOfLedger> getListOfEntriesOfLedger(BookieId address,
                                                                                     long ledgerId) {
        FutureGetListOfEntriesOfLedger futureResult = new FutureGetListOfEntriesOfLedger(ledgerId);
        executor.executeOrdered(address, () ->
                futureResult.completeExceptionally(
                        BKException.create(BKException.Code.IllegalOpException).fillInStackTrace())
        );
        return futureResult;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
    }

    public static ByteBuf copyData(ReferenceCounted rc) {
        ByteBuf res;
        if (rc instanceof ByteBuf) {
            res = Unpooled.copiedBuffer((ByteBuf) rc);
        } else {
            res = ByteBufList.coalesce((ByteBufList) rc);
        }

        return res;
    }

    public static ByteBuf copyDataWithSkipHeader(ReferenceCounted rc) {
        ByteBuf res;
        if (rc instanceof ByteBuf) {
            res = Unpooled.copiedBuffer((ByteBuf) rc);
        } else {
            res = ByteBufList.coalesce((ByteBufList) rc);
        }

        // Skip headers
        res.skipBytes(28);
        rc.release();

        return res;
    }
}
