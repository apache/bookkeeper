package org.apache.bookkeeper.bookie;

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Strictly for testing.
 * have to be in org.apache.bookkeeper.bookie to not introduce changes to InterleavedLedgerStorage
 */
public class SlowInterleavedLedgerStorage extends InterleavedLedgerStorage {

    public static final String PROP_SLOW_STORAGE_FLUSH_DELAY = "test.slowStorage.flushDelay";
    public static final String PROP_SLOW_STORAGE_ADD_DELAY = "test.slowStorage.addDelay";
    public static final String PROP_SLOW_STORAGE_GET_DELAY = "test.slowStorage.getDelay";

    /**
     * Strictly for testing.
     */
    public static class SlowDefaultEntryLogger extends DefaultEntryLogger {
        public volatile long getDelay = 0;
        public volatile long addDelay = 0;
        public volatile long flushDelay = 0;

        public SlowDefaultEntryLogger(ServerConfiguration conf,
                                      LedgerDirsManager ledgerDirsManager,
                                      EntryLogListener listener,
                                      StatsLogger statsLogger) throws IOException {
            super(conf, ledgerDirsManager, listener, statsLogger, UnpooledByteBufAllocator.DEFAULT);
        }

        public SlowDefaultEntryLogger setAddDelay(long delay) {
            addDelay = delay;
            return this;
        }

        public SlowDefaultEntryLogger setGetDelay(long delay) {
            getDelay = delay;
            return this;
        }

        public SlowDefaultEntryLogger setFlushDelay(long delay) {
            flushDelay = delay;
            return this;
        }

        @Override
        public void flush() throws IOException {
            delayMs(flushDelay);
            super.flush();
        }

        @Override
        public long addEntry(long ledger, ByteBuf entry) throws IOException {
            delayMs(addDelay);
            return super.addEntry(ledger, entry);
        }

        @Override
        public ByteBuf readEntry(long ledgerId, long entryId, long location)
                throws IOException, Bookie.NoEntryException {
            delayMs(getDelay);
            return super.readEntry(ledgerId, entryId, location);
        }

        private static void delayMs(long delay) {
            if (delay < 1) {
                return;
            }
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                //noop
            }
        }

    }

    public SlowInterleavedLedgerStorage() {
        super();
    }

    @Override
    public void initialize(ServerConfiguration conf,
                           LedgerManager ledgerManager,
                           LedgerDirsManager ledgerDirsManager,
                           LedgerDirsManager indexDirsManager,
                           StatsLogger statsLogger,
                           ByteBufAllocator allocator)
            throws IOException {
        super.initialize(conf, ledgerManager, ledgerDirsManager, indexDirsManager,
                         statsLogger, allocator);
        // do not want to add these to config class, reading throw "raw" interface
        long getDelay = conf.getLong(PROP_SLOW_STORAGE_GET_DELAY, 0);
        long addDelay = conf.getLong(PROP_SLOW_STORAGE_ADD_DELAY, 0);
        long flushDelay = conf.getLong(PROP_SLOW_STORAGE_FLUSH_DELAY, 0);

        entryLogger = new SlowDefaultEntryLogger(conf, ledgerDirsManager, this, statsLogger)
                .setAddDelay(addDelay)
                .setGetDelay(getDelay)
                .setFlushDelay(flushDelay);
    }

    public void setAddDelay(long delay) {
        ((SlowDefaultEntryLogger) entryLogger).setAddDelay(delay);
    }

    public void setGetDelay(long delay) {
        ((SlowDefaultEntryLogger) entryLogger).setGetDelay(delay);
    }

    public void setFlushDelay(long delay) {
        ((SlowDefaultEntryLogger) entryLogger).setFlushDelay(delay);
    }

}
