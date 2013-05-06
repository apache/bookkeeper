package org.apache.bookkeeper.client;

import java.util.Enumeration;

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

public interface AsyncCallback {
    public interface AddCallback {
        /**
         * Callback declaration
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
    }

    public interface CloseCallback {
        /**
         * Callback definition
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

    public interface CreateCallback {
        /**
         * Declaration of callback method
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

    public interface OpenCallback {
        /**
         * Callback for asynchronous call to open ledger
         *
         * @param rc
         *          Return code
         * @param lh
         *          ledger handle
         * @param ctx
         *          context object
         */

        public void openComplete(int rc, LedgerHandle lh, Object ctx);

    }

    public interface ReadCallback {
        /**
         * Callback declaration
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

    public interface DeleteCallback {
        /**
         * Callback definition for delete operations
         *
         * @param rc
         *          return code
         * @param ctx
         *          context object
         */
        void deleteComplete(int rc, Object ctx);
    }

    public interface ReadLastConfirmedCallback {
        /**
         * Callback definition for bookie recover operations
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

    public interface RecoverCallback {
        /**
         * Callback definition for bookie recover operations
         *
         * @param rc
         *          return code
         * @param ctx
         *          context object
         */
        void recoverComplete(int rc, Object ctx);
    }
    
    public interface IsClosedCallback {
        /**
         * Callback definition for isClosed operation
         *
         * @param rc
         *          return code
         * @param isClosed
         *          true if ledger is closed
         */
        void isClosedComplete(int rc, boolean isClosed, Object ctx);
    }
}
