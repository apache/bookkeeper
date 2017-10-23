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
package org.apache.bookkeeper.stats.twitter.finagle;

import com.twitter.finagle.stats.StatsReceiver;
import org.apache.bookkeeper.stats.Counter;

/**
 * Note: this counter doesn't support resetting values or getting the current value.
 * It also has a limitation in size: max bound is signed integer, not long.
 */
class CounterImpl implements Counter {
    private final com.twitter.finagle.stats.Counter counter;

    public CounterImpl(final String name,
                       final StatsReceiver stats) {
        this.counter = stats.counter0(name);
    }

    @Override
    public void clear() { /* not supported */ }

    @Override
    public void inc() {
        this.counter.incr();
    }

    @Override
    public void dec() {
        this.counter.incr(-1);
    }

    @Override
    public void add(final long delta) {
        if (delta < Integer.MIN_VALUE || delta > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("This counter doesn't support long values");
        }
        this.counter.incr((int) delta);
    }

    @Override
    public Long get() {
        return null; // not supported
    }
}
