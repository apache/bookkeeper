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

package org.apache.bookkeeper.util;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.ReadHandle;

import org.apache.bookkeeper.net.BookieId;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;

/**
 * Test utilities.
 */
@Slf4j
public final class TestUtils {

    private TestUtils() {}

    public static String buildStatsCounterPathFromBookieID(BookieId bookieId) {
        return bookieId.toString().replace('.', '_').replace('-', '_').replace(":", "_");
    }

    public static boolean hasLogFiles(File ledgerDirectory, boolean partial, Integer... logsId) {
        boolean result = partial ? false : true;
        Set<Integer> logs = new HashSet<Integer>();
        for (File file : BookieImpl.getCurrentDirectory(ledgerDirectory).listFiles()) {
            if (file.isFile()) {
                String name = file.getName();
                if (!name.endsWith(".log")) {
                    continue;
                }
                logs.add(Integer.parseInt(name.split("\\.")[0], 16));
            }
        }
        for (Integer logId : logsId) {
            boolean exist = logs.contains(logId);
            if ((partial && exist)
                    || (!partial && !exist)) {
                return !result;
            }
        }
        return result;
    }

    public static void waitUntilLacUpdated(ReadHandle rh, long newLac) throws Exception {
        long lac = rh.getLastAddConfirmed();
        while (lac < newLac) {
            TimeUnit.MILLISECONDS.sleep(20);
            lac = rh.readLastAddConfirmed();
        }
    }

    public static long waitUntilExplicitLacUpdated(LedgerHandle rh, long newLac) throws Exception {
        long lac;
        while ((lac = rh.readExplicitLastConfirmed()) < newLac) {
            TimeUnit.MILLISECONDS.sleep(20);
        }
        return lac;
    }

    public static void assertEventuallyTrue(String description, BooleanSupplier predicate) throws Exception {
        assertEventuallyTrue(description, predicate, 10, TimeUnit.SECONDS);
    }

    public static void assertEventuallyTrue(String description, BooleanSupplier predicate,
                                            long duration, TimeUnit unit) throws Exception {
        long iterations = unit.toMillis(duration) / 100;
        for (int i = 0; i < iterations && !predicate.getAsBoolean(); i++) {
            Thread.sleep(100);
        }
        Assert.assertTrue(description, predicate.getAsBoolean());
    }

    public static int countNumOfFiles(File[] folderNames, String... extensions) {
        int count = 0;
        for (int i = 0; i < folderNames.length; i++) {
            Collection<File> filesCollection = FileUtils.listFiles(folderNames[i], extensions, true);
            count += filesCollection.size();
        }
        return count;
    }

}
