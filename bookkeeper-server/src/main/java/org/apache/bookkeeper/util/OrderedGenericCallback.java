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
package org.apache.bookkeeper.util;

import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import org.apache.bookkeeper.common.util.MdcUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Generic callback implementation which will run the
 * callback in the thread which matches the ordering key.
 */
public abstract class OrderedGenericCallback<T> implements GenericCallback<T> {
    private static final Logger LOG = LoggerFactory.getLogger(OrderedGenericCallback.class);

    private final OrderedExecutor executor;
    private final long orderingKey;
    private final Map<String, String> mdcContextMap;

    /**
     * @param executor The executor on which to run the callback
     * @param orderingKey Key used to decide which thread the callback
     *                    should run on.
     */
    public OrderedGenericCallback(OrderedExecutor executor, long orderingKey) {
        this.executor = executor;
        this.orderingKey = orderingKey;
        this.mdcContextMap = executor.preserveMdc() ? MDC.getCopyOfContextMap() : null;
    }

    @Override
    public final void operationComplete(final int rc, final T result) {
        MdcUtils.restoreContext(mdcContextMap);
        try {
            // during closing, callbacks that are error out might try to submit to
            // the scheduler again. if the submission will go to same thread, we
            // don't need to submit to executor again. this is also an optimization for
            // callback submission
            if (Thread.currentThread().getId() == executor.getThreadID(orderingKey)) {
                safeOperationComplete(rc, result);
            } else {
                try {
                    executor.executeOrdered(orderingKey, new SafeRunnable() {
                        @Override
                        public void safeRun() {
                            safeOperationComplete(rc, result);
                        }

                        @Override
                        public String toString() {
                            return String.format("Callback(key=%s, name=%s)",
                                    orderingKey,
                                    OrderedGenericCallback.this);
                        }
                    });
                } catch (RejectedExecutionException re) {
                    LOG.warn("Failed to submit callback for {} : ", orderingKey, re);
                }
            }
        } finally {
            MDC.clear();
        }
    }

    public abstract void safeOperationComplete(int rc, T result);
}
