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
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.bookkeeper.common.util.MathUtils;

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
        int capacity = MathUtils.findNextPositivePowerOfTwo(initialCapacity);
        data = (T[]) new Object[capacity];
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
            // There is a single consumer thread, so no need to use signalAll()
            notEmpty.signal();
        }
    }

    // must be called while holding the lock
    @SuppressWarnings("unchecked")
    private void grow(int minCapacity) {
        if (minCapacity < 0) {
            // The requested capacity overflowed the int range
            throw new IllegalStateException("Queue capacity would exceed the maximum array size");
        }

        int newCapacity = data.length;
        while (newCapacity < minCapacity) {
            newCapacity *= 2;
            if (newCapacity <= 0) {
                // Doubling overflowed: fall back to the exact requested capacity
                newCapacity = minCapacity;
            }
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
        Objects.requireNonNull(e);

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
        Objects.requireNonNull(a);
        Objects.checkFromIndexSize(offset, len, a.length);

        lock.lock();

        try {
            if (len > data.length - size) {
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
                // There is a single consumer thread, so no need to use signalAll()
                notEmpty.signal();
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
        Objects.requireNonNull(c);
        if (c == this) {
            throw new IllegalArgumentException("Cannot drain a queue into itself");
        }
        if (maxElements <= 0) {
            return 0;
        }

        lock.lock();
        try {
            int toDrain = Math.min(size, maxElements);

            int capacity = data.length;
            int consumerIdx = this.consumerIdx;
            int drained = 0;

            try {
                while (drained < toDrain) {
                    T item = data[consumerIdx];
                    c.add(item);

                    // Only clear the slot once the item was accepted by the target collection
                    data[consumerIdx] = null;
                    if (++consumerIdx == capacity) {
                        consumerIdx = 0;
                    }
                    ++drained;
                }
            } finally {
                // Even if c.add() threw, commit the items that were actually transferred
                this.consumerIdx = consumerIdx;
                size -= drained;
            }

            return drained;
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
        if (array.length == 0) {
            return 0;
        }

        long remainingTimeNanos = unit.toNanos(timeout);

        lock.lockInterruptibly();
        try {
            while (size == 0) {
                if (waitForever) {
                    notEmpty.await();
                } else {
                    if (remainingTimeNanos <= 0L) {
                        return 0;
                    }

                    remainingTimeNanos = notEmpty.awaitNanos(remainingTimeNanos);
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
