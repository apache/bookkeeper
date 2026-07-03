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
package org.apache.bookkeeper.clients.impl.kv;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.grpc.CallOptions;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.Txn;
import org.apache.bookkeeper.api.kv.op.CompareResult;
import org.apache.bookkeeper.api.kv.result.TxnResult;
import org.apache.bookkeeper.clients.exceptions.InternalServerException;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.utils.ClientConstants;
import org.apache.bookkeeper.clients.utils.RetryUtils;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.TableServiceImplBase;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.junit.Test;

/**
 * Unit test of {@link PByteBufSimpleTableImpl}.
 */
public class PByteBufSimpleTableImplTest extends GrpcClientTestBase {

    private static final long STREAM_ID = 1234L;

    private StreamProperties streamProps;

    @Override
    protected void doSetup() throws Exception {
        streamProps = new StreamProperties();
        streamProps.setStreamId(STREAM_ID);
    }

    @Override
    protected void doTeardown() throws Exception {
    }

    private PTable<ByteBuf, ByteBuf> newTable() {
        return new PByteBufSimpleTableImpl(
            streamProps,
            InProcessChannelBuilder.forName(serverName).directExecutor().build(),
            CallOptions.DEFAULT,
            RetryUtils.create(ClientConstants.DEFAULT_BACKOFF_POLICY, scheduler));
    }

    /**
     * Serializing a request drains the ByteBuf slices stored in it, so a request instance must
     * not be reused across RPC attempts. Verify that a retried txn commit sends an intact
     * request, rebuilt for every attempt.
     */
    @Test
    public void testTxnRetrySendsIntactRequest() throws Exception {
        List<TxnRequest> receivedRequests = new CopyOnWriteArrayList<>();
        TableServiceImplBase tableService = new TableServiceImplBase() {
            @Override
            public void txn(TxnRequest request,
                            StreamObserver<TxnResponse> responseObserver) {
                receivedRequests.add(request);
                if (receivedRequests.size() == 1) {
                    // fail the first attempt with a retryable status to force a retry
                    responseObserver.onError(Status.UNAVAILABLE.asRuntimeException());
                } else {
                    TxnResponse response = new TxnResponse();
                    response.setHeader().setCode(StatusCode.SUCCESS);
                    response.setSucceeded(true);
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            }
        };
        serviceRegistry.addService(tableService.bindService());

        PTable<ByteBuf, ByteBuf> table = newTable();
        ByteBuf key = Unpooled.wrappedBuffer("txn-key".getBytes(UTF_8));
        ByteBuf value = Unpooled.wrappedBuffer("txn-value".getBytes(UTF_8));
        Txn<ByteBuf, ByteBuf> txn = table.txn(key);
        CompletableFuture<TxnResult<ByteBuf, ByteBuf>> commitFuture = txn
            .If(
                table.opFactory().compareValue(CompareResult.EQUAL, key, Unpooled.wrappedBuffer(new byte[0]))
            )
            .Then(
                table.opFactory().newPut(key, value, table.opFactory().optionFactory().newPutOption().build())
            )
            .commit();
        try (TxnResult<ByteBuf, ByteBuf> txnResult = FutureUtils.result(commitFuture)) {
            assertTrue(txnResult.isSuccess());
        }

        assertEquals(2, receivedRequests.size());
        for (TxnRequest received : receivedRequests) {
            assertEquals(1, received.getComparesCount());
            assertArrayEquals("txn-key".getBytes(UTF_8), received.getCompareAt(0).getKey());
            assertEquals(1, received.getSuccessesCount());
            assertArrayEquals("txn-key".getBytes(UTF_8), received.getSuccessAt(0).getRequestPut().getKey());
            assertArrayEquals("txn-value".getBytes(UTF_8), received.getSuccessAt(0).getRequestPut().getValue());
            assertArrayEquals("txn-key".getBytes(UTF_8), received.getHeader().getRKey());
            assertEquals(STREAM_ID, received.getHeader().getStreamId());
        }
    }

    /**
     * A server-side error response must surface as an exception instead of being conflated
     * with a legitimately failed txn compare (isSuccess() == false).
     */
    @Test
    public void testTxnServerErrorNotConflatedWithFailedCompare() throws Exception {
        TableServiceImplBase tableService = new TableServiceImplBase() {
            @Override
            public void txn(TxnRequest request,
                            StreamObserver<TxnResponse> responseObserver) {
                TxnResponse response = new TxnResponse();
                response.setHeader().setCode(StatusCode.INTERNAL_SERVER_ERROR);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
        serviceRegistry.addService(tableService.bindService());

        PTable<ByteBuf, ByteBuf> table = newTable();
        ByteBuf key = Unpooled.wrappedBuffer("txn-key".getBytes(UTF_8));
        CompletableFuture<TxnResult<ByteBuf, ByteBuf>> commitFuture = table.txn(key)
            .If(
                table.opFactory().compareValue(CompareResult.EQUAL, key, Unpooled.wrappedBuffer(new byte[0]))
            )
            .commit();
        try {
            FutureUtils.result(commitFuture);
            fail("txn commit should fail when the server responds with an error code");
        } catch (InternalServerException e) {
            // expected
        }
    }

}
