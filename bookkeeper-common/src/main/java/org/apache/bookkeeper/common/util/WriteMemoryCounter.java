/**
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
 */
package org.apache.bookkeeper.common.util;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class WriteMemoryCounter {
    private final WriteWaterMark writeWaterMark;
    private AtomicLong sizeCounter = new AtomicLong(0);
    private AtomicBoolean writeState = new AtomicBoolean(true);
    private final List<WritableListener> listeners = new LinkedList<>();

    public WriteMemoryCounter(WriteWaterMark writeWaterMark) {
        this.writeWaterMark = writeWaterMark;
    }

    public WriteMemoryCounter() {
        this.writeWaterMark = new WriteWaterMark();
    }

    public void register(WritableListener listener) {
        listeners.add(listener);
    }

    public void incrementPendingWriteBytes(long size) {
        long usage = sizeCounter.addAndGet(size);
        log.info("increment the size to {}", usage);
        if (usage > writeWaterMark.high() && writeState.get()) {
            setWritable(false);
        }
    }

    public void decrementPendingWriteBytes(long size) {
        long usage = sizeCounter.addAndGet(-size);
        log.info("decrement the size to {}", usage);
        if (usage < writeWaterMark.low() && !writeState.get()) {
            setWritable(true);
        }
    }

    public void setWritable(boolean state) {
        writeState.set(state);
        listeners.forEach(l -> l.onWriteStateChanged(state));
    }
}
