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
package org.apache.hedwig.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.CompositeException;

public class CallbackUtils {

    /**
     * A callback that waits for all of a number of events to fire. If any fail,
     * then fail the final callback with a composite exception.
     *
     * TODO: change this to use any Exception and make CompositeException
     * generic, not a PubSubException.
     *
     * @param expected
     *            Number of expected callbacks.
     * @param cb
     *            The final callback to call.
     * @param ctx
     * @param logger
     *            May be null.
     * @param successMsg
     *            If not null, then this is logged on success.
     * @param failureMsg
     *            If not null, then this is logged on failure.
     * @param eagerErrorHandler
     *            If not null, then this will be executed after the first
     *            failure (but before the final failure callback). Useful for
     *            releasing resources, etc. as soon as we know the composite
     *            operation is doomed.
     * @return the generated callback
     */
    public static Callback<Void> multiCallback(final int expected, final Callback<Void> cb, final Object ctx,
            final Logger logger, final String successMsg, final String failureMsg,
            Runnable eagerErrorHandler) {
        if (expected == 0) {
            cb.operationFinished(ctx, null);
            return null;
        } else {
            return new Callback<Void>() {

                final AtomicInteger done = new AtomicInteger();
                final LinkedBlockingQueue<PubSubException> exceptions = new LinkedBlockingQueue<PubSubException>();

                private void tick() {
                    if (done.incrementAndGet() == expected) {
                        if (exceptions.isEmpty()) {
                            cb.operationFinished(ctx, null);
                        } else {
                            cb.operationFailed(ctx, new CompositeException(exceptions));
                        }
                    }
                }

                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    if (logger != null && failureMsg != null)
                        logger.error(failureMsg, exception);
                    exceptions.add(exception);
                    tick();
                }

                @Override
                public void operationFinished(Object ctx, Void resultOfOperation) {
                    if (logger != null && successMsg != null)
                        logger.info(successMsg);
                    tick();
                }

            };
        }
    }

    /**
     * A callback that waits for all of a number of events to fire. If any fail,
     * then fail the final callback with a composite exception.
     */
    public static Callback<Void> multiCallback(int expected, Callback<Void> cb, Object ctx) {
        return multiCallback(expected, cb, ctx, null, null, null, null);
    }

    /**
     * A callback that waits for all of a number of events to fire. If any fail,
     * then fail the final callback with a composite exception.
     */
    public static Callback<Void> multinCallback(int expected, Callback<Void> cb, Object ctx, Runnable eagerErrorHandler) {
        return multiCallback(expected, cb, ctx, null, null, null, eagerErrorHandler);
    }

    private static Callback<Void> nop = new Callback<Void>() {

        @Override
        public void operationFailed(Object ctx, PubSubException exception) {
        }

        @Override
        public void operationFinished(Object ctx, Void resultOfOperation) {
        }

    };

    /**
     * A do-nothing callback.
     */
    public static Callback<Void> nop() {
        return nop;
    }

    /**
     * Logs what happened before continuing the callback chain.
     */
    public static <T> Callback<T> logger(final Logger logger, final String successMsg,
                                         final String failureMsg, final Callback<T> cont) {
        return new Callback<T>() {

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                logger.error(failureMsg, exception);
                if (cont != null)
                    cont.operationFailed(ctx, exception);
            }

            @Override
            public void operationFinished(Object ctx, T resultOfOperation) {
                logger.info(successMsg);
                if (cont != null)
                    cont.operationFinished(ctx, resultOfOperation);
            }

        };
    }

    /**
     * Logs what happened (no continuation).
     */
    public static Callback<Void> logger(Logger logger, String successMsg, String failureMsg) {
        return logger(logger, successMsg, failureMsg, nop());
    }

    /**
     * Return a Callback<Void> that just calls the given Callback cb with the
     * bound result.
     */
    public static <T> Callback<Void> curry(final Callback<T> cb, final T result) {
        return new Callback<Void>() {

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                cb.operationFailed(ctx, exception);
            }

            @Override
            public void operationFinished(Object ctx, Void resultOfOperation) {
                cb.operationFinished(ctx, result);
            }

        };
    }

}
