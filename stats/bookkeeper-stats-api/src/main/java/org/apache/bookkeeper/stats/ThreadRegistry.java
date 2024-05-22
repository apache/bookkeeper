/*
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
package org.apache.bookkeeper.stats;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For mapping thread ids to thread pools and threads within those pools
 * or just for lone named threads. Thread scoped metrics add labels to
 * metrics by retrieving the ThreadPoolThread object from this registry.
 * For flexibility, this registry is not based on TLS.
 */
public class ThreadRegistry {
    private static Logger logger = LoggerFactory.getLogger(ThreadRegistry.class);
    private static ConcurrentMap<Long, ThreadPoolThread> threadPoolMap = new ConcurrentHashMap<>();
    private static ConcurrentMap<String, Integer> threadPoolThreadMap = new ConcurrentHashMap<>();

    /*
    Threads can register themselves as their first act before carrying out
    any work. By calling this method, the ThreadPoolThread is incremented
    for the given thread pool.
    */
    public static void register(String threadPool) {
        register(threadPool, false);
    }

    public static void register(String threadPool, boolean force) {
        Integer threadPoolThread = threadPoolThreadMap.compute(threadPool, (k, v) -> v == null ? 0 : v + 1);
        if (force) {
            threadPoolMap.remove(Thread.currentThread().getId());
        }
        register(threadPool, threadPoolThread, Thread.currentThread().getId());
    }

    /**
     * In some tests we run in the same thread activities that should
     * run in different threads from different thread-pools
     * this would trigger assertions to fail.
     * This is a convenience method to work around such cases.
     * This method shouldn't be used in production code.
     */
    public static void forceClearRegistrationForTests(long threadId) {
        threadPoolMap.compute(threadId, (id, value) -> {
            if (value !=  null) {
                logger.info("Forcibly clearing registry entry {} for thread id {}", value, id);
            }
           return null;
        });
    }

    /*
        Threads can register themselves as their first act before carrying out
        any work.
     */
    public static void register(String threadPool, int threadPoolThread) {
        register(threadPool, threadPoolThread, Thread.currentThread().getId());
    }

    /*
        Thread factories can register a thread by its id.
        The assumption is that one thread belongs only to one threadpool.
        The doesn't hold in tests, in which we use mock Executors that
        run the code in the same thread as the caller
     */
    public static void register(String threadPool, int threadPoolThread, long threadId) {
        ThreadPoolThread tpt = new ThreadPoolThread(threadPool, threadPoolThread, threadId);
        ThreadPoolThread previous = threadPoolMap.put(threadId, tpt);
        if (previous != null) {
            throw new IllegalStateException("Thread " + threadId + " was already registered in thread pool "
                    + previous.threadPool + " as thread " + previous.ordinal + " with threadId " + previous.threadId
                    + " trying to overwrite with " + threadPool + " and ordinal " + threadPoolThread);
        }
    }

    public static Runnable registerThread(Runnable runnable, String threadPool) {
        return new RegisteredRunnable(threadPool, runnable);
    }

    /*
        Clears all stored thread state.
     */
    public static void clear() {
        threadPoolMap.clear();
        threadPoolThreadMap.clear();
    }

    /*
        Retrieves the registered ThreadPoolThread (if registered) for the calling thread.
     */
    public static ThreadPoolThread get() {
        return threadPoolMap.get(Thread.currentThread().getId());
    }

    /**
     * Stores the thread pool and ordinal.
     */
    public static final class ThreadPoolThread {
        final String threadPool;
        final int ordinal;
        final long threadId;

        public ThreadPoolThread(String threadPool, int ordinal, long threadId) {
            this.threadPool = threadPool;
            this.ordinal = ordinal;
            this.threadId = threadId;
        }

        public String getThreadPool() {
            return threadPool;
        }

        public int getOrdinal() {
            return ordinal;
        }
    }

    private static class RegisteredRunnable implements Runnable {
        private final String threadPool;
        private final Runnable runnable;

        public RegisteredRunnable(String threadPool, Runnable runnable) {
            this.threadPool = threadPool;
            this.runnable = runnable;
        }

        @Override
        public void run() {
            register(threadPool);
            runnable.run();
        }
    }
}
