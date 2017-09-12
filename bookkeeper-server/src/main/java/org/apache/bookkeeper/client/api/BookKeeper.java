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
package org.apache.bookkeeper.client.api;

import org.apache.bookkeeper.client.BKException;

/**
 * This is the entry point for BookKeeper client side API
 */
public interface BookKeeper extends AutoCloseable {

    /**
     * Start the creation of a new ledger
     *
     * @return a builder for the new ledger
     */
    CreateBuilder createLedger();

    /**
     * Open an existing ledger
     *
     * @return a builder useful to create a readable handler for an existing ledger
     */
    OpenBuilder openLedger();

    /**
     * Delete an existing ledger
     *
     * @return a builder useful to delete an existing ledger
     */
    DeleteBuilder deleteLedger();

    /**
     * Close the client and release every resource
     *
     * @throws BKException
     * @throws InterruptedException
     */
    @Override
    void close() throws BKException, InterruptedException;

}
