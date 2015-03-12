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

package org.apache.bookkeeper.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.BookieShell.UpdateLedgerNotifier;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Encapsulates updating the ledger metadata operation
 */
public class UpdateLedgerOp {

    private final static Logger LOG = LoggerFactory.getLogger(UpdateLedgerOp.class);
    private final BookKeeper bkc;
    private final BookKeeperAdmin admin;

    public UpdateLedgerOp(final BookKeeper bkc, final BookKeeperAdmin admin) {
        this.bkc = bkc;
        this.admin = admin;
    }

    /**
     * Update the bookie id present in the ledger metadata.
     *
     * @param oldBookieId
     *            current bookie id
     * @param newBookieId
     *            new bookie id
     * @param rate
     *            number of ledgers updating per second (default 5 per sec)
     * @param limit
     *            maximum number of ledgers to update (default: no limit). Stop
     *            update if reaching limit
     * @param progressable
     *            report progress of the ledger updates
     * @throws IOException
     *             if there is an error when updating bookie id in ledger
     *             metadata
     * @throws InterruptedException
     *             interrupted exception when update ledger meta
     */
    public void updateBookieIdInLedgers(final BookieSocketAddress oldBookieId, final BookieSocketAddress newBookieId,
            final int rate, final int limit, final UpdateLedgerNotifier progressable) throws BKException, IOException {

        final ThreadFactoryBuilder tfb = new ThreadFactoryBuilder().setNameFormat("UpdateLedgerThread").setDaemon(true);
        final ExecutorService executor = Executors.newSingleThreadExecutor(tfb.build());
        final AtomicInteger issuedLedgerCnt = new AtomicInteger();
        final AtomicInteger updatedLedgerCnt = new AtomicInteger();
        final Future<?> updateBookieCb = executor.submit(new Runnable() {

            @Override
            public void run() {
                updateLedgers(oldBookieId, newBookieId, rate, limit, progressable);
            }

            private void updateLedgers(final BookieSocketAddress oldBookieId, final BookieSocketAddress newBookieId,
                    final int rate, final int limit, final UpdateLedgerNotifier progressable) {
                try {
                    final AtomicBoolean stop = new AtomicBoolean(false);
                    final Set<Long> outstandings = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
                    final RateLimiter throttler = RateLimiter.create(rate);
                    final Iterator<Long> ledgerItr = admin.listLedgers().iterator();
                    final CountDownLatch syncObj = new CountDownLatch(1);

                    // iterate through all the ledgers
                    while (ledgerItr.hasNext() && !stop.get()) {
                        // throttler to control updates per second
                        throttler.acquire();

                        final Long lId = ledgerItr.next();
                        final ReadLedgerMetadataCb readCb = new ReadLedgerMetadataCb(bkc, lId, oldBookieId, newBookieId);
                        outstandings.add(lId);

                        FutureCallback<Void> updateLedgerCb = new UpdateLedgerCb(lId, stop, issuedLedgerCnt,
                                updatedLedgerCnt, outstandings, syncObj, progressable);
                        Futures.addCallback(readCb.getFutureListener(), updateLedgerCb);

                        issuedLedgerCnt.incrementAndGet();
                        if (limit != Integer.MIN_VALUE && issuedLedgerCnt.get() >= limit || !ledgerItr.hasNext()) {
                            stop.set(true);
                        }
                        bkc.getLedgerManager().readLedgerMetadata(lId, readCb);
                    }
                    // waiting till all the issued ledgers are finished
                    syncObj.await();
                } catch (IOException ioe) {
                    LOG.error("Exception while updating ledger", ioe);
                    throw new RuntimeException("Exception while updating ledger", ioe.getCause());
                } catch (InterruptedException ie) {
                    LOG.error("Exception while updating ledger metadata", ie);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Exception while updating ledger", ie.getCause());
                }
            }
        });
        try {
            // Wait to finish the issued ledgers.
            updateBookieCb.get();
        } catch (ExecutionException ee) {
            throw new IOException("Exception while updating ledger", ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Exception while updating ledger", ie);
        } finally {
            executor.shutdown();
        }
    }

    private final static class UpdateLedgerCb implements FutureCallback<Void> {
        final long ledgerId;
        final AtomicBoolean stop;
        final AtomicInteger issuedLedgerCnt;
        final AtomicInteger updatedLedgerCnt;
        final Set<Long> outstandings;
        final CountDownLatch syncObj;
        final UpdateLedgerNotifier progressable;

        public UpdateLedgerCb(long ledgerId, AtomicBoolean stop, AtomicInteger issuedLedgerCnt,
                AtomicInteger updatedLedgerCnt, Set<Long> outstandings, CountDownLatch syncObj,
                UpdateLedgerNotifier progressable) {
            this.ledgerId = ledgerId;
            this.stop = stop;
            this.issuedLedgerCnt = issuedLedgerCnt;
            this.updatedLedgerCnt = updatedLedgerCnt;
            this.outstandings = outstandings;
            this.syncObj = syncObj;
            this.progressable = progressable;
        }

        @Override
        public void onFailure(Throwable th) {
            LOG.error("Error updating ledger {}", ledgerId, th);
            stop.set(true);
            finishUpdateLedger();
        }

        @Override
        public void onSuccess(Void obj) {
            updatedLedgerCnt.incrementAndGet();
            // may print progress
            progressable.progress(updatedLedgerCnt.get(), issuedLedgerCnt.get());
            finishUpdateLedger();
        }

        private void finishUpdateLedger() {
            outstandings.remove(ledgerId);
            if (outstandings.isEmpty() && stop.get()) {
                LOG.info("Total number of ledgers issued={} updated={}", issuedLedgerCnt.get(), updatedLedgerCnt.get());
                syncObj.countDown();
            }
        }
    }

    private final static class ReadLedgerMetadataCb implements GenericCallback<LedgerMetadata> {
        final BookKeeper bkc;
        final Long ledgerId;
        final BookieSocketAddress curBookieAddr;
        final BookieSocketAddress toBookieAddr;
        SettableFuture<Void> future = SettableFuture.create();
        public ReadLedgerMetadataCb(BookKeeper bkc, Long ledgerId, BookieSocketAddress curBookieAddr,
                BookieSocketAddress toBookieAddr) {
            this.bkc = bkc;
            this.ledgerId = ledgerId;
            this.curBookieAddr = curBookieAddr;
            this.toBookieAddr = toBookieAddr;
        }

        ListenableFuture<Void> getFutureListener() {
            return future;
        }

        @Override
        public void operationComplete(int rc, LedgerMetadata metadata) {
            if (BKException.Code.NoSuchLedgerExistsException == rc) {
                future.set(null);
                return; // this is OK
            } else if (BKException.Code.OK != rc) {
                // open ledger failed.
                LOG.error("Get ledger metadata {} failed. Error code {}", ledgerId, rc);
                future.setException(BKException.create(rc));
                return;
            }
            boolean updateEnsemble = false;
            for (ArrayList<BookieSocketAddress> ensembles : metadata.getEnsembles().values()) {
                int index = ensembles.indexOf(curBookieAddr);
                if (-1 != index) {
                    ensembles.set(index, toBookieAddr);
                    updateEnsemble = true;
                }
            }
            if (!updateEnsemble) {
                future.set(null);
                return; // ledger doesn't contains the given curBookieId
            }
            final GenericCallback<Void> writeCb = new GenericCallback<Void>() {
                @Override
                public void operationComplete(int rc, Void result) {
                    if (rc != BKException.Code.OK) {
                        // metadata update failed
                        LOG.error("Ledger {} metadata update failed. Error code {}", ledgerId, rc);
                        future.setException(BKException.create(rc));
                        return;
                    }
                    future.set(null);
                }
            };
            bkc.getLedgerManager().writeLedgerMetadata(ledgerId, metadata, writeCb);
        }
    }
}
