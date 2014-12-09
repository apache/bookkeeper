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
package org.apache.bookkeeper.test;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import com.google.common.util.concurrent.AbstractFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Callbacks implemented with SettableFuture, to be used in tests
 */
public class TestCallbacks {

    private static final Logger logger = LoggerFactory.getLogger(TestCallbacks.class);

    public static class GenericCallbackFuture<T>
        extends AbstractFuture<T> implements GenericCallback<T> {
        @Override
        public void operationComplete(int rc, T value) {
            if (rc != BKException.Code.OK) {
                setException(BKException.create(rc));
            } else {
                set(value);
            }
        }
    }

    public static class AddCallbackFuture
        extends AbstractFuture<Long> implements AddCallback {

        private final long expectedEntryId;

        public AddCallbackFuture(long entryId) {
            this.expectedEntryId = entryId;
        }

        public long getExpectedEntryId() {
            return expectedEntryId;
        }

        @Override
        public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
            logger.info("Add entry {} completed : entryId = {}, rc = {}",
                    new Object[] { expectedEntryId, entryId, rc });
            if (rc != BKException.Code.OK) {
                setException(BKException.create(rc));
            } else {
                set(entryId);
            }
        }
    }
}

