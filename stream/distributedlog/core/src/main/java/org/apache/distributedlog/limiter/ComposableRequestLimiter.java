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
package org.apache.distributedlog.limiter;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.exceptions.OverCapacityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collect rate limiter implementation, cost(RequestT), overlimit, etc. behavior.
 */
public class ComposableRequestLimiter<RequestT> implements RequestLimiter<RequestT> {
    protected static final Logger LOG = LoggerFactory.getLogger(ComposableRequestLimiter.class);

    private final RateLimiter limiter;
    private final OverlimitFunction<RequestT> overlimitFunction;
    private final CostFunction<RequestT> costFunction;
    private final Counter overlimitCounter;

    /**
     * OverlimitFunction.
     */
    public interface OverlimitFunction<RequestT> {
        void apply(RequestT request) throws OverCapacityException;
    }
    /**
     * CostFunction.
     */
    public interface CostFunction<RequestT> {
        int apply(RequestT request);
    }

    public ComposableRequestLimiter(
            RateLimiter limiter,
            OverlimitFunction<RequestT> overlimitFunction,
            CostFunction<RequestT> costFunction,
            StatsLogger statsLogger) {
        checkNotNull(limiter);
        checkNotNull(overlimitFunction);
        checkNotNull(costFunction);
        this.limiter = limiter;
        this.overlimitFunction = overlimitFunction;
        this.costFunction = costFunction;
        this.overlimitCounter = statsLogger.getCounter("overlimit");
    }

    @Override
    public void apply(RequestT request) throws OverCapacityException {
        int permits = costFunction.apply(request);
        if (!limiter.acquire(permits)) {
            overlimitCounter.inc();
            overlimitFunction.apply(request);
        }
    }
}
