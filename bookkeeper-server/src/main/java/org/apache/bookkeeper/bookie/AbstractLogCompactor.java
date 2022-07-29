/**
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

package org.apache.bookkeeper.bookie;

import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.conf.ServerConfiguration;

/**
 * Abstract entry log compactor used for compaction.
 */
public abstract class AbstractLogCompactor {

    protected final ServerConfiguration conf;
    protected final Throttler throttler;

    /**
     * LogRemovalListener.
     */
    public interface LogRemovalListener {
        void removeEntryLog(long logToRemove);
    }

    protected final LogRemovalListener logRemovalListener;

    public AbstractLogCompactor(ServerConfiguration conf, LogRemovalListener logRemovalListener) {
        this.conf = conf;
        this.throttler = new Throttler(conf);
        this.logRemovalListener = logRemovalListener;
    }

    /**
     * Compact entry log file.
     * @param entryLogMeta log metadata for the entry log to be compacted
     * @return true for succeed
     */
    public abstract boolean compact(EntryLogMetadata entryLogMeta);

    /**
     * Do nothing by default. Intended for subclass to override this method.
     */
    public void cleanUpAndRecover() {}

    /**
     * class Throttler.
     */
    public static class Throttler {
        private final RateLimiter rateLimiter;
        private final boolean isThrottleByBytes;
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        Throttler(ServerConfiguration conf) {
            this.isThrottleByBytes  = conf.getIsThrottleByBytes();
            this.rateLimiter = RateLimiter.create(this.isThrottleByBytes
                ? conf.getCompactionRateByBytes() : conf.getCompactionRateByEntries());
        }

        // acquire. if bybytes: bytes of this entry; if byentries: 1.
        boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
            return rateLimiter.tryAcquire(this.isThrottleByBytes ? permits : 1, timeout, unit);
        }

        // GC thread will check the status for the rate limiter
        // If the compactor is being stopped by other threads,
        // and the GC thread is still limited, the compact task will be stopped.
        public void acquire(int permits) throws IOException {
            long timeout = 100;
            long start = System.currentTimeMillis();
            while (!tryAcquire(permits, timeout, TimeUnit.MILLISECONDS)) {
                if (cancelled.get()) {
                    throw new IOException("Failed to get permits takes "
                            + (System.currentTimeMillis() - start)
                            + " ms may be compactor has been shutting down");
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(timeout);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }

        public void cancelledAcquire() {
            cancelled.set(true);
        }
    }

}
