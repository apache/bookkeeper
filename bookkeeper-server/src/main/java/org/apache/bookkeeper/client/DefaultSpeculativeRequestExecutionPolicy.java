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
package org.apache.bookkeeper.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default implementation of {@link SpeculativeRequestExecutionPolicy}.
 *
 * <p>The policy issues speculative requests in a backoff way. The time between two speculative requests
 * are between {@code firstSpeculativeRequestTimeout} and {@code maxSpeculativeRequestTimeout}.
 */
public class DefaultSpeculativeRequestExecutionPolicy implements SpeculativeRequestExecutionPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSpeculativeRequestExecutionPolicy.class);
    final int firstSpeculativeRequestTimeout;
    final int maxSpeculativeRequestTimeout;
    final float backoffMultiplier;

    public DefaultSpeculativeRequestExecutionPolicy(int firstSpeculativeRequestTimeout,
            int maxSpeculativeRequestTimeout, float backoffMultiplier) {
        this.firstSpeculativeRequestTimeout = firstSpeculativeRequestTimeout;
        this.maxSpeculativeRequestTimeout = maxSpeculativeRequestTimeout;
        this.backoffMultiplier = backoffMultiplier;

        if (backoffMultiplier <= 0) {
            throw new IllegalArgumentException("Invalid value provided for backoffMultiplier");
        }

        // Prevent potential over flow
        if (Math.round((double) maxSpeculativeRequestTimeout * (double) backoffMultiplier) > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid values for maxSpeculativeRequestTimeout and backoffMultiplier");
        }
    }

    /**
     * Initialize the speculative request execution policy.
     *
     * @param scheduler The scheduler service to issue the speculative request
     * @param requestExecutor The executor is used to issue the actual speculative requests
     * @return ScheduledFuture, in case caller needs to cancel it.
     */
    @Override
    public ScheduledFuture<?> initiateSpeculativeRequest(final ScheduledExecutorService scheduler,
            final SpeculativeRequestExecutor requestExecutor) {
        return scheduleSpeculativeRead(scheduler, requestExecutor, firstSpeculativeRequestTimeout);
    }

    private ScheduledFuture<?> scheduleSpeculativeRead(final ScheduledExecutorService scheduler,
                                         final SpeculativeRequestExecutor requestExecutor,
                                         final int speculativeRequestTimeout) {
        try {
            return scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    ListenableFuture<Boolean> issueNextRequest = requestExecutor.issueSpeculativeRequest();
                    Futures.addCallback(issueNextRequest, new FutureCallback<Boolean>() {
                        // we want this handler to run immediately after we push the big red button!
                        @Override
                        public void onSuccess(Boolean issueNextRequest) {
                            if (issueNextRequest) {
                                scheduleSpeculativeRead(scheduler, requestExecutor,
                                        Math.min(maxSpeculativeRequestTimeout,
                                        Math.round((float) speculativeRequestTimeout * backoffMultiplier)));
                            } else {
                                if (LOG.isTraceEnabled()) {
                                    LOG.trace("Stopped issuing speculative requests for {}, "
                                        + "speculativeReadTimeout = {}", requestExecutor, speculativeRequestTimeout);
                                }
                            }
                        }

                        @Override
                        public void onFailure(Throwable thrown) {
                            LOG.warn("Failed to issue speculative request for {}, speculativeReadTimeout = {} : ",
                                    requestExecutor, speculativeRequestTimeout, thrown);
                        }
                    });
                }
            }, speculativeRequestTimeout, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException re) {
            if (!scheduler.isShutdown()) {
                LOG.warn("Failed to schedule speculative request for {}, speculativeReadTimeout = {} : ",
                        requestExecutor, speculativeRequestTimeout, re);
            }
        }
        return null;
    }
}
