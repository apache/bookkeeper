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
package org.apache.bookkeeper.common.collections;

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This implements a {@link BlockingQueue} backed by an array with no fixed capacity.
 *
 * <p>It is the unbounded companion of {@link BatchedArrayBlockingQueue}: when the backing array
 * is full it is doubled in size (and never shrunk), so producers never block.
 *
 * <p>This queue only allows 1 consumer thread to dequeue items and multiple producer threads.
 */
public class GrowableBatchedArrayBlockingQueue<T>
        extends AbstractQueue<T>
        implements BlockingQueue<T>, BatchedBlockingQueue<T> {

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition notEmpty = lock.newCondition();

    private T[] data;

    private int size;

    private int consumerIdx;
    private int producerIdx;

    public GrowableBatchedArrayBlockingQueue() {
        this(64);
    }

    @SuppressWarnings("unchecked")
    public GrowableBatchedArrayBlockingQueue(int initialCapacity) {
        data = (T[]) new Object[initialCapacity];
    }

    private T dequeueOne() {
        T item = data[consumerIdx];
        data[consumerIdx] = null;
        if (++consumerIdx == data.length) {
            consumerIdx = 0;
        }

        --size;
        return item;
    }

    private void enqueueOne(T item) {
        if (size == data.length) {
            grow(size + 1);
        }

        data[producerIdx] = item;
        if (++producerIdx == data.length) {
            producerIdx = 0;
        }

        if (size++ == 0) {
            notEmpty.signalAll();
        }
    }

    // must be called while holding the lock
    @SuppressWarnings("unchecked")
    private void grow(int minCapacity) {
        int newCapacity = data.length;
        while (newCapacity < minCapacity) {
            newCapacity *= 2;
        }

        T[] newData = (T[]) new Object[newCapacity];

        int firstSpan = Math.min(size, data.length - consumerIdx);
        System.arraycopy(data, consumerIdx, newData, 0, firstSpan);

        int secondSpan = size - firstSpan;
        if (secondSpan > 0) {
            System.arraycopy(data, 0, newData, firstSpan, secondSpan);
        }

        data = newData;
        consumerIdx = 0;
        producerIdx = size;
    }

    @Override
    public T poll() {
        lock.lock();

        try {
            if (size == 0) {
                return null;
            }

            return dequeueOne();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public T peek() {
        lock.lock();

        try {
            if (size == 0) {
                return null;
            }

            return data[consumerIdx];
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(T e) {
        lock.lock();

        try {
            enqueueOne(e);

            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(T e) {
        offer(e);
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) {
        // Queue is unbounded and it will never reject new items
        return offer(e);
    }

    @Override
    public void putAll(T[] a, int offset, int len) {
        lock.lock();

        try {
            if (size + len > data.length) {
                grow(size + len);
            }

            int capacity = data.length;
            int producerIdx = this.producerIdx;

            // First span
            int firstSpan = Math.min(len, capacity - producerIdx);
            System.arraycopy(a, offset, data, producerIdx, firstSpan);
            producerIdx += firstSpan;

            int secondSpan = len - firstSpan;
            if (secondSpan > 0) {
                System.arraycopy(a, offset + firstSpan, data, 0, secondSpan);
                producerIdx = secondSpan;
            }

            if (producerIdx == capacity) {
                producerIdx = 0;
            }

            this.producerIdx = producerIdx;

            if (size == 0) {
                notEmpty.signalAll();
            }

            size += len;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public T take() throws InterruptedException {
        lock.lockInterruptibly();

        try {
            while (size == 0) {
                notEmpty.await();
            }

            return dequeueOne();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        long remainingTimeNanos = unit.toNanos(timeout);

        lock.lockInterruptibly();
        try {
            while (size == 0) {
                if (remainingTimeNanos <= 0L) {
                    return null;
                }

                remainingTimeNanos = notEmpty.awaitNanos(remainingTimeNanos);
            }

            return dequeueOne();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        lock.lock();
        try {
            int toDrain = Math.min(size, maxElements);

            int capacity = data.length;
            int consumerIdx = this.consumerIdx;
            for (int i = 0; i < toDrain; i++) {
                T item = data[consumerIdx];
                data[consumerIdx] = null;
                c.add(item);

                if (++consumerIdx == capacity) {
                    consumerIdx = 0;
                }
            }

            this.consumerIdx = consumerIdx;
            size -= toDrain;
            return toDrain;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int takeAll(T[] array) throws InterruptedException {
        return internalTakeAll(array, true, 0, TimeUnit.SECONDS);
    }

    @Override
    public int pollAll(T[] array, long timeout, TimeUnit unit) throws InterruptedException {
        return internalTakeAll(array, false, timeout, unit);
    }

    private int internalTakeAll(T[] array, boolean waitForever, long timeout, TimeUnit unit)
            throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (size == 0) {
                if (waitForever) {
                    notEmpty.await();
                } else {
                    if (!notEmpty.await(timeout, unit)) {
                        return 0;
                    }
                }
            }

            int toDrain = Math.min(size, array.length);

            int capacity = data.length;
            int consumerIdx = this.consumerIdx;

            // First span
            int firstSpan = Math.min(toDrain, capacity - consumerIdx);
            System.arraycopy(data, consumerIdx, array, 0, firstSpan);
            Arrays.fill(data, consumerIdx, consumerIdx + firstSpan, null);
            consumerIdx += firstSpan;

            int secondSpan = toDrain - firstSpan;
            if (secondSpan > 0) {
                System.arraycopy(data, 0, array, firstSpan, secondSpan);
                Arrays.fill(data, 0, secondSpan, null);
                consumerIdx = secondSpan;
            }

            if (consumerIdx == capacity) {
                consumerIdx = 0;
            }
            this.consumerIdx = consumerIdx;

            size -= toDrain;
            return toDrain;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            while (size > 0) {
                dequeueOne();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        lock.lock();

        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }
}
