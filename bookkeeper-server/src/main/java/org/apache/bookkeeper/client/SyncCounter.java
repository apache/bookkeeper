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

import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;

/**
 * Implements objects to help with the synchronization of asynchronous calls
 *
 */

class SyncCounter {
    int i;
    int rc;
    Enumeration<LedgerEntry> seq = null;
    LedgerHandle lh = null;
    private CountDownLatch latch = new CountDownLatch(1);

    void dec() {
        latch.countDown();
    }

    void block() throws InterruptedException {
        latch.await();
    }

    void setrc(int rc) {
        this.rc = rc;
    }

    int getrc() {
        return rc;
    }

    void setSequence(Enumeration<LedgerEntry> seq) {
        this.seq = seq;
    }

    Enumeration<LedgerEntry> getSequence() {
        return seq;
    }

    void setLh(LedgerHandle lh) {
        this.lh = lh;
    }

    LedgerHandle getLh() {
        return lh;
    }
}
