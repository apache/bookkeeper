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
package org.apache.distributedlog;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

class BKTransmitPacket {

    private final EntryBuffer recordSet;
    private final long transmitTime;
    private final CompletableFuture<Integer> transmitComplete;

    BKTransmitPacket(EntryBuffer recordSet) {
        this.recordSet = recordSet;
        this.transmitTime = System.nanoTime();
        this.transmitComplete = new CompletableFuture<Integer>();
    }

    EntryBuffer getRecordSet() {
        return recordSet;
    }

    CompletableFuture<Integer> getTransmitFuture() {
        return transmitComplete;
    }

    /**
     * Complete the transmit with result code <code>transmitRc</code>.
     *
     * <p>It would notify all the waiters that are waiting via {@link #awaitTransmitComplete(long, TimeUnit)}
     * or {@link #addTransmitCompleteListener(FutureEventListener)}.
     *
     * @param transmitResult
     *          transmit result code.
     */
    public void notifyTransmitComplete(int transmitResult) {
        transmitComplete.complete(transmitResult);
    }

    /**
     * Register a transmit complete listener.
     *
     * <p>The listener will be triggered with transmit result when transmit completes.
     * The method should be non-blocking.
     *
     * @param transmitCompleteListener
     *          listener on transmit completion
     * @see #awaitTransmitComplete(long, TimeUnit)
     */
    void addTransmitCompleteListener(FutureEventListener<Integer> transmitCompleteListener) {
        transmitComplete.whenComplete(transmitCompleteListener);
    }

    /**
     * Await for the transmit to be complete.
     *
     * @param timeout
     *          wait timeout
     * @param unit
     *          wait timeout unit
     */
    int awaitTransmitComplete(long timeout, TimeUnit unit)
        throws Exception {
        return FutureUtils.result(transmitComplete, timeout, unit);
    }

    public long getTransmitTime() {
        return transmitTime;
    }

}
