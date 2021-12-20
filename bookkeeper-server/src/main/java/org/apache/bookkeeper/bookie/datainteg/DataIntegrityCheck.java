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

package org.apache.bookkeeper.bookie.datainteg;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * The interface for the data integrity check feature. This feature allows
 * a bookie to handle data loss scenarios such as when running without
 * the journal or after a disk failure has caused the loss of all data.
 */
public interface DataIntegrityCheck {
    /**
     * Run quick preboot check. This check should do enough to ensure that
     * it is safe to complete the boot sequence without compromising correctness.
     * To this end, if it finds that this bookie is part of the last ensemble of
     * an unclosed ledger, it must prevent the bookie from being able store new
     * entries for that ledger and must prevent the bookie from taking part in
     * the discovery of the last entry of that ledger.
     */
    CompletableFuture<Void> runPreBootCheck(String reason);

    /**
     * Whether we need to run a full check.
     * This condition can be set by the runPreBoot() call to run a full check
     * in the background once the bookie is running. This can later be used
     * to run the full check periodically, or to exponentially backoff and retry
     * when some transient condition prevents a ledger being fixed during a
     * full check.
     */
    boolean needsFullCheck() throws IOException;

    /**
     * Run full check of bookies local data. This check should ensure that
     * if the metadata service states that it should have an entry, then it
     * should have that entry. If the entry is missing, it should copy it
     * from another available source.
     */
    CompletableFuture<Void> runFullCheck();
}
