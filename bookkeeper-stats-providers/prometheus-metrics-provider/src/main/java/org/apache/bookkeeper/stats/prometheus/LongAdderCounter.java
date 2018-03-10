package org.apache.bookkeeper.stats.prometheus;

import java.util.concurrent.atomic.LongAdder;

import org.apache.bookkeeper.stats.Counter;

/**
 * {@link Counter} implementation based on {@link LongAdder}.
 *
 * <p>LongAdder keeps a counter per-thread and then aggregates to get the result, in order to avoid contention between
 * multiple threads.
 */
public class LongAdderCounter implements Counter {
    private final LongAdder counter = new LongAdder();

    @Override
    public void clear() {
        counter.reset();
    }

    @Override
    public void inc() {
        counter.increment();
    }

    @Override
    public void dec() {
        counter.decrement();
    }

    @Override
    public void add(long delta) {
        counter.add(delta);
    }

    @Override
    public Long get() {
        return counter.sum();
    }
}
