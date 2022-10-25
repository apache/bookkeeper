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
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.StampedLock;
import org.apache.bookkeeper.util.MathUtils;


/**
 * This implements a {@link BlockingQueue} backed by an array with no fixed capacity.
 *
 * <p>When the capacity is reached, data will be moved to a bigger array.
 *
 * <p>This queue only allows 1 consumer thread to dequeue items and multiple producer threads.
 */
public class GrowableMpScArrayConsumerBlockingQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {

    private final StampedLock headLock = new StampedLock();
    private final PaddedInt headIndex = new PaddedInt();
    private final PaddedInt tailIndex = new PaddedInt();
    private final StampedLock tailLock = new StampedLock();

    private T[] data;
    private final AtomicInteger size = new AtomicInteger(0);

    private volatile Thread waitingConsumer;

    public GrowableMpScArrayConsumerBlockingQueue() {
        this(64);
    }

    @SuppressWarnings("unchecked")
    public GrowableMpScArrayConsumerBlockingQueue(int initialCapacity) {
        int capacity = MathUtils.findNextPositivePowerOfTwo(initialCapacity);
        data = (T[]) new Object[capacity];
    }

    @Override
    public T remove() {
        T item = poll();
        if (item == null) {
            throw new NoSuchElementException();
        }

        return item;
    }

    @Override
    public T poll() {
        if (size.get() > 0) {
            // Since this is a single-consumer queue, we don't expect multiple threads calling poll(), though we need
            // to protect against array expansions
            long stamp = headLock.readLock();

            try {
                T item = data[headIndex.value];
                data[headIndex.value] = null;
                headIndex.value = (headIndex.value + 1) & (data.length - 1);
                size.decrementAndGet();
                return item;
            } finally {
                headLock.unlockRead(stamp);
            }
        } else {
            return null;
        }
    }

    @Override
    public T element() {
        T item = peek();
        if (item == null) {
            throw new NoSuchElementException();
        }

        return item;
    }

    @Override
    public T peek() {
        if (size.get() > 0) {
            long stamp = headLock.readLock();

            try {
                return data[headIndex.value];
            } finally {
                headLock.unlockRead(stamp);
            }
        } else {
            return null;
        }
    }

    @Override
    public boolean offer(T e) {
        // Queue is unbounded and it will never reject new items
        put(e);
        return true;
    }

    @Override
    public void put(T e) {
        long stamp = tailLock.writeLock();

        try {
            int oldSize = size.get();
            if (oldSize == data.length) {
                expandArray();
            }

            data[tailIndex.value] = e;
            tailIndex.value = (tailIndex.value + 1) & (data.length - 1);

            if (size.getAndIncrement() == 0 && waitingConsumer != null) {
                Thread waitingConsumer = this.waitingConsumer;
                this.waitingConsumer = null;
                LockSupport.unpark(waitingConsumer);
            }
        } finally {
            tailLock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean add(T e) {
        put(e);
        return true;
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) {
        // Queue is unbounded and it will never reject new items
        put(e);
        return true;
    }

    @Override
    public T take() throws InterruptedException {
        while (size() == 0) {
            waitingConsumer = Thread.currentThread();

            // Double check that size has not changed after we have registered ourselves for notification
            if (size() == 0) {
                LockSupport.park();
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
            }
        }

        return poll();
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);

        while (size.get() == 0) {
            waitingConsumer = Thread.currentThread();

            // Double check that size has not changed after we have registered ourselves for notification
            if (size.get() == 0) {
                LockSupport.parkUntil(deadline);
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }

                if (System.currentTimeMillis() >= deadline) {
                    return null;
                }
            }
        }

        return poll();
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
        long stamp = headLock.readLock();

        try {
            int toDrain = Math.min(size.get(), maxElements);

            for (int i = 0; i < toDrain; i++) {
                T item = data[headIndex.value];
                data[headIndex.value] = null;
                c.add(item);

                headIndex.value = (headIndex.value + 1) & (data.length - 1);
            }

            this.size.addAndGet(-toDrain);
            return toDrain;
        } finally {
            headLock.unlockRead(stamp);
        }
    }

    @Override
    public void clear() {
        long stamp = headLock.readLock();

        try {
            int size = this.size.get();

            for (int i = 0; i < size; i++) {
                data[headIndex.value] = null;
                headIndex.value = (headIndex.value + 1) & (data.length - 1);
            }

            this.size.addAndGet(-size);
        } finally {
            headLock.unlockRead(stamp);
        }
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        long tailStamp = tailLock.writeLock();
        long headStamp = headLock.writeLock();

        try {
            int headIndex = this.headIndex.value;
            int size = this.size.get();

            sb.append('[');

            for (int i = 0; i < size; i++) {
                T item = data[headIndex];
                if (i > 0) {
                    sb.append(", ");
                }

                sb.append(item);

                headIndex = (headIndex + 1) & (data.length - 1);
            }

            sb.append(']');
        } finally {
            headLock.unlockWrite(headStamp);
            tailLock.unlockWrite(tailStamp);
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private void expandArray() {
        // We already hold the tailLock
        long headLockStamp = headLock.writeLock();

        try {
            int size = this.size.get();
            int newCapacity = data.length * 2;
            T[] newData = (T[]) new Object[newCapacity];


            int oldHeadIndex = headIndex.value;
            int lenHeadToEnd = Math.min(size, data.length - oldHeadIndex);

            System.arraycopy(data, oldHeadIndex, newData, 0, lenHeadToEnd);
            System.arraycopy(data, 0, newData, lenHeadToEnd, size - lenHeadToEnd);

            data = newData;
            headIndex.value = 0;
            tailIndex.value = size;
        } finally {
            headLock.unlockWrite(headLockStamp);
        }
    }

    private static final class PaddedInt {
        int value = 0;

        // Padding to avoid false sharing
        public volatile int pi1 = 1;
        public volatile long p1 = 1L, p2 = 2L, p3 = 3L, p4 = 4L, p5 = 5L, p6 = 6L;

        public long exposeToAvoidOptimization() {
            return pi1 + p1 + p2 + p3 + p4 + p5 + p6;
        }
    }
}
