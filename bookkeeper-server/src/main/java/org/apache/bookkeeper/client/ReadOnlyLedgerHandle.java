package org.apache.bookkeeper.client;

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

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import java.security.GeneralSecurityException;
import java.net.InetSocketAddress;

/**
 * Read only ledger handle. This ledger handle allows you to 
 * read from a ledger but not to write to it. It overrides all 
 * the public write operations from LedgerHandle.
 * It should be returned for BookKeeper#openLedger operations.
 */
class ReadOnlyLedgerHandle extends LedgerHandle {
    ReadOnlyLedgerHandle(BookKeeper bk, long ledgerId, LedgerMetadata metadata,
                         DigestType digestType, byte[] password)
            throws GeneralSecurityException, NumberFormatException {
        super(bk, ledgerId, metadata, digestType, password);
    }

    @Override
    public void close() 
            throws InterruptedException, BKException {
        // noop
    }

    @Override
    public void asyncClose(CloseCallback cb, Object ctx) {
        cb.closeComplete(BKException.Code.OK, this, ctx);
    }
    
    @Override
    public long addEntry(byte[] data) throws InterruptedException, BKException {
        return addEntry(data, 0, data.length);
    }
    
    @Override
    public long addEntry(byte[] data, int offset, int length)
            throws InterruptedException, BKException {
        LOG.error("Tried to add entry on a Read-Only ledger handle, ledgerid=" + ledgerId);        
        throw BKException.create(BKException.Code.IllegalOpException);
    }

    @Override
    public void asyncAddEntry(final byte[] data, final AddCallback cb,
                              final Object ctx) {
        asyncAddEntry(data, 0, data.length, cb, ctx);
    }

    @Override
    public void asyncAddEntry(final byte[] data, final int offset, final int length,
                              final AddCallback cb, final Object ctx) {
        LOG.error("Tried to add entry on a Read-Only ledger handle, ledgerid=" + ledgerId);
        cb.addComplete(BKException.Code.IllegalOpException, this,
                       LedgerHandle.INVALID_ENTRY_ID, ctx);
    }

    @Override
    void handleBookieFailure(final InetSocketAddress addr, final int bookieIndex) {
        blockAddCompletions.incrementAndGet();
        synchronized (metadata) {
            try {
                if (!metadata.currentEnsemble.get(bookieIndex).equals(addr)) {
                    // ensemble has already changed, failure of this addr is immaterial
                    LOG.debug("Write did not succeed to {}, bookieIndex {},"
                              +" but we have already fixed it.", addr, bookieIndex);
                    blockAddCompletions.decrementAndGet();
                    return;
                }

                replaceBookieInMetadata(addr, bookieIndex);

                blockAddCompletions.decrementAndGet();
                // the failed bookie has been replaced
                unsetSuccessAndSendWriteRequest(bookieIndex);
            } catch (BKException.BKNotEnoughBookiesException e) {
                LOG.error("Could not get additional bookie to "
                          + "remake ensemble, closing ledger: " + ledgerId);
                handleUnrecoverableErrorDuringAdd(e.getCode());
                return;
            }
        }
    }
}
