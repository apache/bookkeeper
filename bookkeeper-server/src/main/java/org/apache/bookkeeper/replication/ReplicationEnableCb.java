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
package org.apache.bookkeeper.replication;

import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Callback which is getting notified when the replication process is enabled.
 */
public class ReplicationEnableCb implements GenericCallback<Void> {

    private static final Logger LOG = LoggerFactory
            .getLogger(ReplicationEnableCb.class);
    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void operationComplete(int rc, Void result) {
        latch.countDown();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Automatic ledger re-replication is enabled");
        }
    }

    /**
     * This is a blocking call and causes the current thread to wait until the
     * replication process is enabled.
     *
     * @throws InterruptedException
     *             interrupted while waiting
     */
    public void await() throws InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Automatic ledger re-replication is disabled. Hence waiting until its enabled!");
        }
        latch.await();
    }
}
