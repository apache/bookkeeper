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
package org.apache.bookkeeper.bookie;

import io.netty.util.concurrent.FastThreadLocalThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper that wraps bookie threads.
 * Any common handing that we require for all bookie threads
 * should be implemented here
 */
public class BookieThread extends FastThreadLocalThread implements
        Thread.UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory
            .getLogger(BookieThread.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        handleException(t, e);
    }

    public BookieThread(String name) {
        super(name);
        setUncaughtExceptionHandler(this);
    }

    public BookieThread(Runnable thread, String name) {
        super(thread, name);
        setUncaughtExceptionHandler(this);
    }

    /**
     * Handles uncaught exception occurred in thread.
     */
    protected void handleException(Thread t, Throwable e) {
        LOG.error("Uncaught exception in thread {}", t.getName(), e);
    }
}
