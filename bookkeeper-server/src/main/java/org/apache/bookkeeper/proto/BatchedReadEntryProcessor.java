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

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import java.util.concurrent.ExecutorService;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.proto.BookieProtocol.BatchedReadRequest;
import org.apache.bookkeeper.util.ByteBufList;

public class BatchedReadEntryProcessor extends ReadEntryProcessor {

    private long maxBatchReadSize;

    public static BatchedReadEntryProcessor create(BatchedReadRequest request,
            BookieRequestHandler requestHandler,
            BookieRequestProcessor requestProcessor,
            ExecutorService fenceThreadPool,
            boolean throttleReadResponses,
            long maxBatchReadSize) {
        BatchedReadEntryProcessor rep = RECYCLER.get();
        rep.init(request, requestHandler, requestProcessor);
        rep.fenceThreadPool = fenceThreadPool;
        rep.throttleReadResponses = throttleReadResponses;
        rep.maxBatchReadSize = maxBatchReadSize;
        requestProcessor.onReadRequestStart(requestHandler.ctx().channel());
        return rep;
    }

    @Override
    protected ReferenceCounted readData() throws Exception {
        ByteBufList data = null;
        BatchedReadRequest batchRequest = (BatchedReadRequest) request;
        int maxCount = batchRequest.getMaxCount();
        if (maxCount <= 0) {
            maxCount = Integer.MAX_VALUE;
        }
        long maxSize = Math.min(batchRequest.getMaxSize(), maxBatchReadSize);
        //See BookieProtoEncoding.ResponseEnDeCoderPreV3#encode on BatchedReadResponse case.
        long frameSize = 24 + 8 + 4;
        for (int i = 0; i < maxCount; i++) {
            try {
                if (data == null) {
                    ByteBuf entry = requestProcessor.getBookie().readEntry(request.getLedgerId(), request.getEntryId());
                    frameSize += entry.readableBytes() + 4;
                    data = ByteBufList.get(entry);
                } else {
                    long remainingEntrySize = maxSize - frameSize;
                    if (remainingEntrySize <= 0) {
                        break;
                    }
                    ByteBuf entry = requestProcessor.getBookie().readEntryIfFits(
                            request.getLedgerId(), request.getEntryId() + i, remainingEntrySize);
                    if (entry == null) {
                        break;
                    }
                    frameSize += entry.readableBytes() + 4;
                    data.add(entry);
                }
            } catch (Bookie.NoEntryException e) {
                if (data == null) {
                    throw e;
                }
                break;
            } catch (Throwable e) {
                if (data != null) {
                    data.release();
                }
                throw e;
            }
        }
        return data;
    }

    @Override
    protected BookieProtocol.Response buildReadResponse(ReferenceCounted data) {
        return ResponseBuilder.buildBatchedReadResponse((ByteBufList) data, (BatchedReadRequest) request);
    }

    @Override
    public String toString() {
        BatchedReadRequest br = (BatchedReadRequest) request;
        return String.format("BatchedReadEntry(%d, %d %d, %d)", br.getLedgerId(), br.getEntryId(), br.getMaxCount(),
                br.getMaxSize());
    }

    protected void recycle() {
        request.recycle();
        super.reset();
        if (this.recyclerHandle != null) {
            this.recyclerHandle.recycle(this);
        }
    }

    private final Recycler.Handle<BatchedReadEntryProcessor> recyclerHandle;

    private BatchedReadEntryProcessor(Recycler.Handle<BatchedReadEntryProcessor> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<BatchedReadEntryProcessor> RECYCLER = new Recycler<BatchedReadEntryProcessor>() {
        @Override
        protected BatchedReadEntryProcessor newObject(Recycler.Handle<BatchedReadEntryProcessor> handle) {
            return new BatchedReadEntryProcessor(handle);
        }
    };

}
