/**
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
package org.apache.bookkeeper.client;

import java.util.Enumeration;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * Defines all the callback interfaces for the async operations in bookkeeper client.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface AsyncCallback {
    /**
     * Async Callback for adding entries to ledgers with latency information.
     *
     * @since 4.7
     */
    @InterfaceAudience.Public
    @InterfaceStability.Evolving
    interface AddCallbackWithLatency {
        /**
         * Callback declaration which additionally passes quorum write complete latency.
         *
         * @param rc
         *          return code
         * @param lh
         *          ledger handle
         * @param entryId
         *          entry identifier
         * @param qwcLatency
         *          QuorumWriteComplete Latency
         * @param ctx
         *          context object
         */
        void addCompleteWithLatency(int rc, LedgerHandle lh, long entryId, long qwcLatency, Object ctx);
    }

    /**
     * Async Callback for adding entries to ledgers.
     *
     * @since 4.0
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    interface AddCallback extends AddCallbackWithLatency {
        /**
         * Callback to implement if latency information is not desired.
         *
         * @param rc
         *          return code
         * @param lh
         *          ledger handle
         * @param entryId
         *          entry identifier
         * @param ctx
         *          context object
         */
        void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx);

        /**
         * Callback declaration which additionally passes quorum write complete latency.
         *
         * @param rc
         *          return code
         * @param lh
         *          ledger handle
         * @param entryId
         *          entry identifier
         * @param qwcLatency
         *          QuorumWriteComplete Latency
         * @param ctx
         *          context object
         */
        @Override
        default void addCompleteWithLatency(int rc, LedgerHandle lh, long entryId, long qwcLatency, Object ctx) {
            addComplete(rc, lh, entryId, ctx);
        }
    }

    /**
     * Async Callback for updating LAC for ledgers.
     *
     * @since 4.5
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    interface AddLacCallback {
        /**
         * Callback declaration.
         *
         * @param rc
         *          return code
         * @param lh
         *          ledger handle
         * @param ctx
         *          context object
         */
        void addLacComplete(int rc, LedgerHandle lh, Object ctx);
    }

    /**
     * Async Callback for closing ledgers.
     *
     * @since 4.0
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    interface CloseCallback {
        /**
         * Callback definition.
         *
         * @param rc
         *          return code
         * @param lh
         *          ledger handle
         * @param ctx
         *          context object
         */
        void closeComplete(int rc, LedgerHandle lh, Object ctx);
    }

    /**
     * Async Callback for creating ledgers.
     *
     * @since 4.0
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    interface CreateCallback {
        /**
         * Declaration of callback method.
         *
         * @param rc
         *          return status
         * @param lh
         *          ledger handle
         * @param ctx
         *          context object
         */
        void createComplete(int rc, LedgerHandle lh, Object ctx);
    }

    /**
     * Async Callback for opening ledgers.
     *
     * @since 4.0
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    interface OpenCallback {
        /**
         * Callback for asynchronous call to open ledger.
         *
         * @param rc
         *          Return code
         * @param lh
         *          ledger handle
         * @param ctx
         *          context object
         */
        void openComplete(int rc, LedgerHandle lh, Object ctx);

    }

    /**
     * Async Callback for reading entries from ledgers.
     *
     * @since 4.0
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    interface ReadCallback {
        /**
         * Callback declaration.
         *
         * @param rc
         *          return code
         * @param lh
         *          ledger handle
         * @param seq
         *          sequence of entries
         * @param ctx
         *          context object
         */
        void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq,
                          Object ctx);
    }

    /**
     * Async Callback for deleting ledgers.
     *
     * @since 4.0
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    interface DeleteCallback {
        /**
         * Callback definition for delete operations.
         *
         * @param rc
         *          return code
         * @param ctx
         *          context object
         */
        void deleteComplete(int rc, Object ctx);
    }

    /**
     * Async Callback for reading LAC for ledgers.
     *
     * @since 4.0
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    interface ReadLastConfirmedCallback {
        /**
         * Callback definition for bookie recover operations.
         *
         * @param rc Return code
         * @param lastConfirmed The entry id of the last confirmed write or
         *                      {@link LedgerHandle#INVALID_ENTRY_ID INVALID_ENTRY_ID}
         *                      if no entry has been confirmed
         * @param ctx
         *          context object
         */
        void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx);
    }

    /**
     * Async Callback for long polling read request.
     *
     * @since 4.5
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    interface ReadLastConfirmedAndEntryCallback {
        /**
         * Callback definition for bookie operation that allows reading the last add confirmed
         * along with an entry within the last add confirmed range.
         *
         * @param rc Return code
         * @param lastConfirmed The entry id of the last confirmed write or
         *                      {@link LedgerHandle#INVALID_ENTRY_ID INVALID_ENTRY_ID}
         *                      if no entry has been confirmed
         * @param entry The entry since the lastAddConfirmed entry that was specified when the request
         *              was initiated
         * @param ctx context object
         */
        void readLastConfirmedAndEntryComplete(int rc, long lastConfirmed, LedgerEntry entry, Object ctx);
    }

    /**
     * Async Callback for recovering ledgers.
     *
     * @since 4.0
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    interface RecoverCallback {
        /**
         * Callback definition for bookie recover operations.
         *
         * @param rc
         *          return code
         * @param ctx
         *          context object
         */
        void recoverComplete(int rc, Object ctx);
    }

    /**
     * Async Callback for checking if a ledger is closed.
     *
     * @since 4.0
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    interface IsClosedCallback {
        /**
         * Callback definition for isClosed operation.
         *
         * @param rc
         *          return code
         * @param isClosed
         *          true if ledger is closed
         */
        void isClosedComplete(int rc, boolean isClosed, Object ctx);
    }
}
