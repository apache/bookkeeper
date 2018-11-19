/*
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
package org.apache.bookkeeper.common.collections;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jctools.queues.MpscArrayQueue;

/**
 * Blocking queue optimized for multiple producers and single consumer.
 */
public class BlockingMpscQueue<T> extends MpscArrayQueue<T> implements BlockingQueue<T> {

    public BlockingMpscQueue(int size) {
        super(size);
    }

    @Override
    public void put(T e) throws InterruptedException {
        while (!this.relaxedOffer(e)) {
            // Do busy-spin loop
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
        long absoluteEndTime = System.nanoTime() + unit.toNanos(timeout);

        while (!this.relaxedOffer(e)) {
            // Do busy-spin loop

            if (System.nanoTime() > absoluteEndTime) {
                return false;
            }

            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }

        return true;
    }

    @Override
    public T take() throws InterruptedException {
        int idleCounter = 0;
        while (true) {
            T item = relaxedPoll();
            if (item == null) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }

                idleCounter = WAIT_STRATEGY.idle(idleCounter);
                continue;
            }


            return item;
        }
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        long absoluteEndTime = System.nanoTime() + unit.toNanos(timeout);

        int idleCounter = 0;
        while (true) {
            T item = relaxedPoll();
            if (item == null) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }

                if (System.nanoTime() > absoluteEndTime) {
                    return null;
                } else {
                    idleCounter = WAIT_STRATEGY.idle(idleCounter);
                    continue;
                }
            }

            return item;
        }
    }

    @Override
    public int remainingCapacity() {
        return capacity() - size();
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        int initialSize = c.size();

        final DrainStrategy ds = new DrainStrategy();
        drain(c::add, ds, ds);
        return c.size() - initialSize;
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        return drain(c::add, maxElements);
    }

    /**
     * Wait strategy combined with exit condition, for draining the queue.
     */
    private static final class DrainStrategy implements WaitStrategy, ExitCondition {

        boolean reachedEnd = false;

        @Override
        public boolean keepRunning() {
            return !reachedEnd;
        }

        @Override
        public int idle(int idleCounter) {
            reachedEnd = true;
            return idleCounter;
        }

    }

    /**
     * Waiting strategy that starts with busy loop and gradually falls back to sleeping if no items are available.
     */
    private static final WaitStrategy SPIN_STRATEGY = new WaitStrategy() {

        @Override
        public int idle(int idleCounter) {
            BusyWait.onSpinWait();
            return idleCounter + 1;
        }
    };

    private static final WaitStrategy WAIT_STRATEGY = SPIN_STRATEGY;
}
