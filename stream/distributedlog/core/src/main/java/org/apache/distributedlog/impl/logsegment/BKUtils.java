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
package org.apache.distributedlog.impl.logsegment;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.common.functions.VoidFunctions;



/**
 * BookKeeper Util Functions.
 */
public class BKUtils {

    /**
     * Close a ledger <i>lh</i>.
     *
     * @param lh ledger handle
     * @return future represents close result.
     */
    public static CompletableFuture<Void> closeLedger(LedgerHandle lh) {
        final CompletableFuture<Void> closePromise = new CompletableFuture<Void>();
        lh.asyncClose(new AsyncCallback.CloseCallback() {
            @Override
            public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
                if (BKException.Code.OK != rc) {
                    FutureUtils.completeExceptionally(closePromise, BKException.create(rc));
                } else {
                    FutureUtils.complete(closePromise, null);
                }
            }
        }, null);
        return closePromise;
    }

    /**
     * Close a list of ledgers <i>lhs</i>.
     *
     * @param lhs a list of ledgers
     * @return future represents close results.
     */
    public static CompletableFuture<Void> closeLedgers(LedgerHandle ... lhs) {
        List<CompletableFuture<Void>> closeResults = Lists.newArrayListWithExpectedSize(lhs.length);
        for (LedgerHandle lh : lhs) {
            closeResults.add(closeLedger(lh));
        }
        return FutureUtils.collect(closeResults).thenApply(VoidFunctions.LIST_TO_VOID_FUNC);
    }

}
