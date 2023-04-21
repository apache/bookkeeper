/*
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
package org.apache.bookkeeper.tests.integration.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thread reaper.
 */
public class ThreadReaper {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadReaper.class);
    private static AtomicInteger groupId = new AtomicInteger(0);

    public static <T> T runWithReaper(Callable<T> callable) throws Exception {
        ThreadGroup tg = new ThreadGroup("reaper-group-" + groupId.incrementAndGet());
        CompletableFuture<T> promise = new CompletableFuture<>();
        Thread t = new Thread(tg, () -> {
                try {
                    promise.complete(callable.call());
                } catch (Throwable ex) {
                    promise.completeExceptionally(ex);
                }
        }, "reapable-thread");
        t.start();
        T ret = promise.get();

        int i = 30; // try to clean up for 3 seconds
        while (tg.activeCount() > 0 && i > 0) {
            tg.interrupt();
            Thread.sleep(100);
            LOG.info("{} threads still alive", tg.activeCount());
            i--;
        }
        if (tg.activeCount() == 0) {
            LOG.info("All threads in reaper group dead");
        } else {
            Thread[] threads = new Thread[tg.activeCount()];
            int found = tg.enumerate(threads);
            LOG.info("Leaked {} threads", found);
            for (int j = 0; j < found; j++) {
                LOG.info("Leaked thread {}", threads[j]);
            }
        }
        return ret;
    }
}
