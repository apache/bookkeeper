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
package org.apache.bookkeeper.metastore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.metastore.MSException.Code;
import org.apache.bookkeeper.versioning.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides utilities for metastore.
 */
public class MetastoreUtils {

    private static final Logger logger = LoggerFactory.getLogger(MetastoreUtils.class);

    static class MultiMetastoreCallback<T> implements MetastoreCallback<T> {

        int rc = Code.OK.getCode();
        final int numOps;
        final AtomicInteger numFinished = new AtomicInteger(0);
        final CountDownLatch doneLatch = new CountDownLatch(1);

        MultiMetastoreCallback(int numOps) {
            this.numOps = numOps;
        }

        @Override
        public void complete(int rc, T value, Object ctx) {
            if (Code.OK.getCode() != rc) {
                this.rc = rc;
                doneLatch.countDown();
                return;
            }
            if (numFinished.incrementAndGet() == numOps) {
                doneLatch.countDown();
            }
        }

        public void waitUntilAllFinished() throws MSException, InterruptedException {
            doneLatch.await();
            if (Code.OK.getCode() != rc) {
                throw MSException.create(Code.get(rc));
            }
        }
    }

    static class SyncMetastoreCallback<T> implements MetastoreCallback<T> {

        int rc;
        T result;
        final CountDownLatch doneLatch = new CountDownLatch(1);

        @Override
        public void complete(int rc, T value, Object ctx) {
            this.rc = rc;
            result = value;
            doneLatch.countDown();
        }

        public T getResult() throws MSException, InterruptedException {
            doneLatch.await();

            if (Code.OK.getCode() != rc) {
                throw MSException.create(Code.get(rc));
            }
            return result;
        }

    }

    /**
     * Clean the given table.
     *
     * @param table
     *          Metastore Table.
     * @param numEntriesPerScan
     *          Num entries per scan.
     * @throws MSException
     * @throws InterruptedException
     */
    public static void cleanTable(MetastoreTable table, int numEntriesPerScan)
    throws MSException, InterruptedException {
        // open cursor
        SyncMetastoreCallback<MetastoreCursor> openCb = new SyncMetastoreCallback<MetastoreCursor>();
        table.openCursor(MetastoreTable.NON_FIELDS, openCb, null);
        MetastoreCursor cursor = openCb.getResult();
        logger.info("Open cursor for table {} to clean entries.", table.getName());

        List<String> keysToClean = new ArrayList<String>(numEntriesPerScan);
        int numEntriesRemoved = 0;
        while (cursor.hasMoreEntries()) {
            logger.info("Fetching next {} entries from table {} to clean.",
                         numEntriesPerScan, table.getName());
            Iterator<MetastoreTableItem> iter = cursor.readEntries(numEntriesPerScan);
            keysToClean.clear();
            while (iter.hasNext()) {
                MetastoreTableItem item = iter.next();
                String key = item.getKey();
                keysToClean.add(key);
            }
            if (keysToClean.isEmpty()) {
                continue;
            }

            logger.info("Issuing deletes to delete keys {}", keysToClean);
            // issue deletes to delete batch of keys
            MultiMetastoreCallback<Void> mcb = new MultiMetastoreCallback<Void>(keysToClean.size());
            for (String key : keysToClean) {
                table.remove(key, Version.ANY, mcb, null);
            }
            mcb.waitUntilAllFinished();
            numEntriesRemoved += keysToClean.size();
            logger.info("Removed {} entries from table {}.", numEntriesRemoved, table.getName());
        }

        logger.info("Finished cleaning up table {}.", table.getName());
    }
}
