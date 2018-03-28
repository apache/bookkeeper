/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.stats.twitter.ostrich;

import org.apache.bookkeeper.stats.Counter;

class CounterImpl implements Counter {

    private final com.twitter.ostrich.stats.Counter ostrichCounter;

    CounterImpl(com.twitter.ostrich.stats.Counter ostrichCounter) {
        this.ostrichCounter = ostrichCounter;
    }

    @Override
    public void clear() {
        this.ostrichCounter.reset();
    }

    @Override
    public void inc() {
        this.ostrichCounter.incr();
    }

    @Override
    public void dec() {
        this.ostrichCounter.incr(-1);
    }

    @Override
    public void add(long delta) {
        this.ostrichCounter.incr((int) delta);
    }

    @Override
    public Long get() {
        return this.ostrichCounter.apply();
    }
}
