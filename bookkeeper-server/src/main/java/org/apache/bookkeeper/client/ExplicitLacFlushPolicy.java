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
package org.apache.bookkeeper.client;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;

import org.apache.bookkeeper.client.LedgerHandle.LastAddConfirmedCallback;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface ExplicitLacFlushPolicy {
    void stopExplicitLacFlush();

    void updatePiggyBackedLac(long piggyBackedLac);

    static final ExplicitLacFlushPolicy VOID_EXPLICITLAC_FLUSH_POLICY = new ExplicitLacFlushPolicy() {
        @Override
        public void stopExplicitLacFlush() {
            // void method
        }

        @Override
        public void updatePiggyBackedLac(long piggyBackedLac) {
            // void method
        }
    };

    class ExplicitLacFlushPolicyImpl implements ExplicitLacFlushPolicy {
        final static Logger LOG = LoggerFactory.getLogger(ExplicitLacFlushPolicyImpl.class);

        volatile long piggyBackedLac = LedgerHandle.INVALID_ENTRY_ID;
        volatile long explicitLac = LedgerHandle.INVALID_ENTRY_ID;
        final LedgerHandle lh;
        ScheduledFuture<?> scheduledFuture;

        ExplicitLacFlushPolicyImpl(LedgerHandle lh) {
            this.lh = lh;
            scheduleExplictLacFlush();
            LOG.debug("Scheduled Explicit Last Add Confirmed Update");
        }

        private long getExplicitLac() {
            return explicitLac;
        }

        private void setExplicitLac(long explicitLac) {
            this.explicitLac = explicitLac;
        }

        private long getPiggyBackedLac() {
            return piggyBackedLac;
        }

        public void setPiggyBackedLac(long piggyBackedLac) {
            this.piggyBackedLac = piggyBackedLac;
        }

        private void scheduleExplictLacFlush() {
            int explicitLacIntervalInSec = lh.bk.getExplicitLacInterval();
            final SafeRunnable updateLacTask = new SafeRunnable() {
                @Override
                public void safeRun() {
                    // Made progress since previous explicitLAC through
                    // Piggyback, so no need to send an explicit LAC update to
                    // bookies.
                    if (getExplicitLac() < getPiggyBackedLac()) {
                        LOG.debug("ledgerid: {}", lh.getId());
                        LOG.debug("explicitLac:{} piggybackLac:{}", getExplicitLac(),
                                getPiggyBackedLac());
                        setExplicitLac(getPiggyBackedLac());
                        return;
                    }

                    if (lh.getLastAddConfirmed() > getExplicitLac()) {
                        // Send Explicit LAC
                        LOG.debug("ledgerid: {}", lh.getId());
                        asyncExplicitLacFlush(lh.getLastAddConfirmed());
                        setExplicitLac(lh.getLastAddConfirmed());
                        LOG.debug("After sending explict LAC lac: {}  explicitLac:{}", lh.getLastAddConfirmed(),
                                getExplicitLac());
                    }
                }

                @Override
                public String toString() {
                    return String.format("UpdateLacTask ledgerId - (%d)", lh.getId());
                }
            };
            try {
                scheduledFuture = lh.bk.mainWorkerPool.scheduleAtFixedRateOrdered(lh.getId(), updateLacTask,
                        explicitLacIntervalInSec, explicitLacIntervalInSec, SECONDS);
            } catch (RejectedExecutionException re) {
                LOG.error("Scheduling of ExplictLastAddConfirmedFlush for ledger: {} has failed because of {}",
                        lh.getId(), re);
            }
        }

        /**
         * Make a LastAddUpdate request.
         */
        void asyncExplicitLacFlush(final long explicitLac) {
            final LastAddConfirmedCallback cb = LastAddConfirmedCallback.INSTANCE;
            final PendingWriteLacOp op = new PendingWriteLacOp(lh, cb, null);
            op.setLac(explicitLac);
            try {
                LOG.debug("Sending Explicit LAC: {}", explicitLac);
                lh.bk.mainWorkerPool.submit(new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        ChannelBuffer toSend = lh.macManager
                                .computeDigestAndPackageForSendingLac(lh.getLastAddConfirmed());
                        op.initiate(toSend);
                    }
                });
            } catch (RejectedExecutionException e) {
                cb.addLacComplete(lh.bk.getReturnRc(BKException.Code.InterruptedException), lh, null);
            }
        }

        @Override
        public void stopExplicitLacFlush() {
            scheduledFuture.cancel(true);
        }

        @Override
        public void updatePiggyBackedLac(long piggyBackedLac) {
            setPiggyBackedLac(piggyBackedLac);
        }
    }
}
