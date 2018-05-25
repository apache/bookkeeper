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

package org.apache.bookkeeper.tests.rpc;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.bookkeeper.tests.proto.rpc.PingPongServiceGrpc.PingPongServiceImplBase;
import org.bookkeeper.tests.proto.rpc.PingRequest;
import org.bookkeeper.tests.proto.rpc.PongResponse;

/**
 * An implementation of the ping pong service used for testing.
 */
@Slf4j
public class PingPongService extends PingPongServiceImplBase {

    private final int streamPongSize;

    public PingPongService(int streamPongSize) {
        this.streamPongSize = streamPongSize;
    }

    @Override
    public void pingPong(PingRequest request, StreamObserver<PongResponse> responseObserver) {
        responseObserver.onNext(PongResponse.newBuilder()
            .setLastSequence(request.getSequence())
            .setNumPingReceived(1)
            .setSlotId(0)
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<PingRequest> lotsOfPings(StreamObserver<PongResponse> responseObserver) {
        return new StreamObserver<PingRequest>() {

            int pingCount = 0;
            long lastSequence = -1L;

            @Override
            public void onNext(PingRequest value) {
                pingCount++;
                lastSequence = value.getSequence();
            }

            @Override
            public void onError(Throwable t) {
                log.error("Failed on receiving stream of pings", t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(PongResponse.newBuilder()
                    .setNumPingReceived(pingCount)
                    .setLastSequence(lastSequence)
                    .setSlotId(0)
                    .build());
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void lotsOfPongs(PingRequest request, StreamObserver<PongResponse> responseObserver) {
        long sequence = request.getSequence();
        for (int i = 0; i < streamPongSize; i++) {
            responseObserver.onNext(PongResponse.newBuilder()
                .setLastSequence(sequence)
                .setNumPingReceived(1)
                .setSlotId(i)
                .build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<PingRequest> bidiPingPong(StreamObserver<PongResponse> responseObserver) {
        return new StreamObserver<PingRequest>() {

            int pingCount = 0;

            @Override
            public void onNext(PingRequest ping) {
                pingCount++;
                responseObserver.onNext(PongResponse.newBuilder()
                    .setLastSequence(ping.getSequence())
                    .setNumPingReceived(pingCount)
                    .setSlotId(0)
                    .build());
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }
}
