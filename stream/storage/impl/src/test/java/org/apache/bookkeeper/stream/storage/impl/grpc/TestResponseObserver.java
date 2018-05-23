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

package org.apache.bookkeeper.stream.storage.impl.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Response Observer used for testing.
 */
public class TestResponseObserver<RespT> implements StreamObserver<RespT> {

    private final AtomicReference<RespT> resultHolder =
        new AtomicReference<>();
    private final AtomicReference<Throwable> exceptionHolder =
        new AtomicReference<>();
    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void onNext(RespT resp) {
        resultHolder.set(resp);
    }

    @Override
    public void onError(Throwable t) {
        exceptionHolder.set(t);
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        latch.countDown();
    }

    public RespT get() throws Throwable {
        latch.await();
        if (null != exceptionHolder.get()) {
            throw exceptionHolder.get();
        }
        return resultHolder.get();
    }

    public void verifySuccess(RespT resp) throws Exception {
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertEquals(resp, resultHolder.get());
    }

    public void verifyException(Status status) throws Exception {
        latch.await();
        assertNull(resultHolder.get());
        assertNotNull(exceptionHolder.get());
        assertTrue(
            exceptionHolder.get() instanceof StatusRuntimeException
                || exceptionHolder.get() instanceof StatusException);
        if (exceptionHolder.get() instanceof StatusRuntimeException) {
            assertEquals(status, ((StatusRuntimeException) exceptionHolder.get()).getStatus());
        } else if (exceptionHolder.get() instanceof StatusException) {
            assertEquals(status, ((StatusException) exceptionHolder.get()).getStatus());
        }
    }
}
