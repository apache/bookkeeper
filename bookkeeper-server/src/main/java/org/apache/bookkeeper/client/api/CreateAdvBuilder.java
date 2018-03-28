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
package org.apache.bookkeeper.client.api;

import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;

/**
 * Builder-style interface to create new ledgers.
 *
 * @since 4.6
 * @see BookKeeper#newCreateLedgerOp()
 */
@Public
@Unstable
public interface CreateAdvBuilder extends OpBuilder<WriteAdvHandle> {

    /**
     * Set a fixed ledgerId for the newly created ledger. If no explicit ledgerId is passed a new ledger id will be
     * assigned automatically.
     *
     * @param ledgerId
     *
     * @return the builder itself
     */
    CreateAdvBuilder withLedgerId(long ledgerId);
}
