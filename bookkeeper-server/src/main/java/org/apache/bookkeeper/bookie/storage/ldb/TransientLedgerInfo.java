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
package org.apache.bookkeeper.bookie.storage.ldb;

import static org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification.WATCHER_RECYCLER;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.common.util.Watchable;
import org.apache.bookkeeper.common.util.Watcher;

/**
 * This class borrows the logic from FileInfo.
 *
 * <p>This class is used for holding all the transient states for a given ledger.
 */
class TransientLedgerInfo extends Watchable<LastAddConfirmedUpdateNotification> implements AutoCloseable {

    static final long LEDGER_INFO_CACHING_TIME_MINUTES = 10;

    static final long NOT_ASSIGNED_LAC = Long.MIN_VALUE;

    // lac
    private volatile long lac = NOT_ASSIGNED_LAC;
    // request from explicit lac requests
    private ByteBuffer explicitLac = null;
    // is the ledger info closed?
    private boolean isClosed;

    private final long ledgerId;
    // reference to LedgerMetadataIndex
    private final LedgerMetadataIndex ledgerIndex;

    private long lastAccessed;

    /**
     * Construct an Watchable with zero watchers.
     */
    public TransientLedgerInfo(long ledgerId, LedgerMetadataIndex ledgerIndex) {
        super(WATCHER_RECYCLER);
        this.ledgerId = ledgerId;
        this.ledgerIndex = ledgerIndex;
        this.lastAccessed = System.currentTimeMillis();
    }

    long getLastAddConfirmed() {
        return lac;
    }

    long setLastAddConfirmed(long lac) {
        long lacToReturn;
        boolean changed = false;
        synchronized (this) {
            if (this.lac == NOT_ASSIGNED_LAC || this.lac < lac) {
                this.lac = lac;
                changed = true;
                lastAccessed = System.currentTimeMillis();
            }
            lacToReturn = this.lac;
        }
        if (changed) {
            notifyWatchers(lacToReturn);
        }
        return lacToReturn;
    }

    synchronized boolean waitForLastAddConfirmedUpdate(long previousLAC,
            Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        lastAccessed = System.currentTimeMillis();
        if ((lac != NOT_ASSIGNED_LAC && lac > previousLAC) || isClosed) {
            return false;
        }

        addWatcher(watcher);
        return true;
    }

    synchronized void cancelWaitForLastAddConfirmedUpdate(Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        deleteWatcher(watcher);
    }

    public ByteBuf getExplicitLac() {
        ByteBuf retLac = null;
        synchronized (this) {
            if (explicitLac != null) {
                retLac = Unpooled.buffer(explicitLac.capacity());
                explicitLac.rewind(); // copy from the beginning
                retLac.writeBytes(explicitLac);
                explicitLac.rewind();
                return retLac;
            }
        }
        return retLac;
    }

    public void setExplicitLac(ByteBuf lac) {
        long explicitLacValue;
        synchronized (this) {
            if (explicitLac == null) {
                explicitLac = ByteBuffer.allocate(lac.capacity());
            }
            lac.readBytes(explicitLac);
            explicitLac.rewind();

            // skip the ledger id
            explicitLac.getLong();
            explicitLacValue = explicitLac.getLong();
            explicitLac.rewind();

            lastAccessed = System.currentTimeMillis();
        }
        setLastAddConfirmed(explicitLacValue);
    }

    boolean isStale() {
        return (lastAccessed + TimeUnit.MINUTES.toMillis(LEDGER_INFO_CACHING_TIME_MINUTES)) < System
                .currentTimeMillis();
    }

    void notifyWatchers(long lastAddConfirmed) {
        notifyWatchers(LastAddConfirmedUpdateNotification.FUNC, lastAddConfirmed);
    }

    @Override
    public void close() {
        synchronized (this) {
            if (isClosed) {
                return;
            }
            isClosed = true;
        }
        // notify watchers
        notifyWatchers(Long.MAX_VALUE);
    }

}
