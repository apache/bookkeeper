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

import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.AsyncCallback;
import org.jboss.netty.buffer.ChannelBuffer;
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

    public interface WriteCallback {
        void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr, Object ctx);
    }

    public interface GenericCallback<T> {
        void operationComplete(int rc, T result);
    }

    /**
     * Declaration of a callback implementation for calls from BookieClient objects.
     * Such calls are for replies of read operations (operations to read an entry
     * from a ledger).
     *
     */

    public interface ReadEntryCallback {
        void readEntryComplete(int rc, long ledgerId, long entryId, ChannelBuffer buffer, Object ctx);
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
        // This keeps track of how many operations have completed
        final AtomicInteger done = new AtomicInteger();
        // List of the exceptions from operations that completed unsuccessfully
        final LinkedBlockingQueue<Integer> exceptions = new LinkedBlockingQueue<Integer>();

        public MultiCallback(int expected, AsyncCallback.VoidCallback cb, Object context, int successRc, int failureRc) {
            this.expected = expected;
            this.cb = cb;
            this.context = context;
            this.failureRc = failureRc;
            this.successRc = successRc;
            if (expected == 0) {
                cb.processResult(successRc, null, context);
            }
        }

        private void tick() {
            if (done.incrementAndGet() == expected) {
                if (exceptions.isEmpty()) {
                    cb.processResult(successRc, null, context);
                } else {
                    cb.processResult(failureRc, null, context);
                }
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
         * @param iterationCallback
         *          Callback to invoke when process has been done.
         */
        public void process(T data, AsyncCallback.VoidCallback cb);
    }

}
