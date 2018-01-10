/*
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
 */
package org.apache.bookkeeper.meta;

import java.io.IOException;

/**
 * Layout manager for writing/reading/deleting ledger layout.
 */
public interface LayoutManager {

    /**
     * The Ledger layout exists exception.
     */
    class LedgerLayoutExistsException extends IOException {

        public LedgerLayoutExistsException(String message) {
            super(message);
        }

        public LedgerLayoutExistsException(String message, Throwable cause) {
            super(message, cause);
        }

        public LedgerLayoutExistsException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * Return the ledger layout.
     *
     * @return the ledger layout
     * @throws IOException when fail to read ledger layout.
     */
    LedgerLayout readLedgerLayout() throws IOException;

    /**
     * Store the ledger layout.
     *
     * @param layout ledger layout
     * @throws IOException when fail to store ledger layout.
     */
    void storeLedgerLayout(LedgerLayout layout) throws IOException;

    /**
     * Delete ledger layout.
     *
     * @throws IOException
     */
    void deleteLedgerLayout() throws IOException;

}
