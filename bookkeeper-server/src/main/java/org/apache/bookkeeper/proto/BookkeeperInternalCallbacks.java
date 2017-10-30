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

package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.zookeeper.AsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Declaration of a callback interfaces used in bookkeeper client library but
 * not exposed to the client application.
 */
public class BookkeeperInternalCallbacks {

    static final Logger LOG = LoggerFactory.getLogger(BookkeeperInternalCallbacks.class);

    /**
     * Callback for calls from BookieClient objects. Such calls are for replies
     * of write operations (operations to add an entry to a ledger).
     *
     */

    /**
     * Listener on ledger metadata changes.
     */
    public interface LedgerMetadataListener {
        /**
         * Triggered each time ledger metadata changed.
         *
         * @param ledgerId
         *          ledger id.
         * @param metadata
         *          new ledger metadata.
         */
        void onChanged(long ledgerId, LedgerMetadata metadata);
    }

    public interface WriteCallback {
        void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx);
    }

    public interface ForceCallback {
        void forceComplete(int rc, long ledgerId, long lastAddSynced, BookieSocketAddress addr, Object ctx);
    }

    public interface ReadLacCallback {
        void readLacComplete(int rc, long ledgerId, ByteBuf lac, ByteBuf buffer, Object ctx);
    }

    public interface WriteLacCallback {
        void writeLacComplete(int rc, long ledgerId, BookieSocketAddress addr, Object ctx);
    }

    public interface StartTLSCallback {
        void startTLSComplete(int rc, Object ctx);
    }

    public interface GenericCallback<T> {
        void operationComplete(int rc, T result);
    }
    
    public static class TimedGenericCallback<T> implements GenericCallback<T> {

        final GenericCallback<T> cb;
        final int successRc;
        final OpStatsLogger statsLogger;
        final long startTime;

        public TimedGenericCallback(GenericCallback<T> cb, int successRc, OpStatsLogger statsLogger) {
            this.cb = cb;
            this.successRc = successRc;
            this.statsLogger = statsLogger;
            this.startTime = MathUtils.nowInNano();
        }

        @Override
        public void operationComplete(int rc, T result) {
            if (successRc == rc) {
                statsLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            } else {
                statsLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            }
            cb.operationComplete(rc, result);
        }
    }

    public interface ReadEntryCallbackCtx {
        void setLastAddConfirmed(long lac);
        long getLastAddConfirmed();
    }

    /**
     * Declaration of a callback implementation for calls from BookieClient objects.
     * Such calls are for replies of read operations (operations to read an entry
     * from a ledger).
     *
     */
    public interface ReadEntryCallback {
        void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx);
    }

    /**
     * Listener on entries responded.
     */
    public interface ReadEntryListener {
        /**
         * On given <i>entry</i> completed.
         *
         * @param rc
         *          result code of reading this entry.
         * @param lh
         *          ledger handle.
         * @param entry
         *          ledger entry.
         * @param ctx
         *          callback context.
         */
        void onEntryComplete(int rc, LedgerHandle lh, LedgerEntry entry, Object ctx);
    }
    
    public interface GetBookieInfoCallback {
        void getBookieInfoComplete(int rc, BookieInfo bInfo, Object ctx);
    }

    /**
     * This is a multi callback object that waits for all of
     * the multiple async operations to complete. If any fail, then we invoke
     * the final callback with a provided failureRc
     */
    public static class MultiCallback implements AsyncCallback.VoidCallback {
        // Number of expected callbacks
        final int expected;
        final int failureRc;
        final int successRc;
        // Final callback and the corresponding context to invoke
        final AsyncCallback.VoidCallback cb;
        final Object context;
        final ExecutorService callbackExecutor;
        // This keeps track of how many operations have completed
        final AtomicInteger done = new AtomicInteger();
        // List of the exceptions from operations that completed unsuccessfully
        final LinkedBlockingQueue<Integer> exceptions = new LinkedBlockingQueue<Integer>();

        public MultiCallback(int expected, AsyncCallback.VoidCallback cb, Object context,
                             int successRc, int failureRc) {
            this(expected, cb, context, successRc, failureRc, null);
        }

        public MultiCallback(int expected, AsyncCallback.VoidCallback cb, Object context,
                             int successRc, int failureRc, ExecutorService callbackExecutor) {
            this.expected = expected;
            this.cb = cb;
            this.context = context;
            this.failureRc = failureRc;
            this.successRc = successRc;
            this.callbackExecutor = callbackExecutor;
            if (expected == 0) {
                callback();
            }
        }

        private void tick() {
            if (done.incrementAndGet() == expected) {
                callback();
            }
        }

        private void callback() {
            if (null != callbackExecutor) {
                try {
                    callbackExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
                            doCallback();
                        }
                    });
                } catch (RejectedExecutionException ree) {
                    // if the callback executor is shutdown, do callback in same thread
                    doCallback();
                }
            } else {
                doCallback();
            }
        }

        private void doCallback() {
            if (exceptions.isEmpty()) {
                cb.processResult(successRc, null, context);
            } else {
                cb.processResult(failureRc, null, context);
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx) {
            if (rc != successRc) {
                LOG.error("Error in multi callback : " + rc);
                exceptions.add(rc);
            }
            tick();
        }

    }

    /**
     * Processor to process a specific element
     */
    public static interface Processor<T> {
        /**
         * Process a specific element
         *
         * @param data
         *          data to process
         * @param cb
         *          Callback to invoke when process has been done.
         */
        public void process(T data, AsyncCallback.VoidCallback cb);
    }

}
