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
package org.apache.bookkeeper.bookie.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.directentrylogger.Events;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.commons.lang3.tuple.Pair;
/**
 * EntryLogIdsImpl.
 */
public class EntryLogIdsImpl implements EntryLogIds {
    public static final Pattern FILE_PATTERN = Pattern.compile("^([0-9a-fA-F]+)\\.log$");
    public static final Pattern COMPACTED_FILE_PATTERN =
            Pattern.compile("^([0-9a-fA-F]+)\\.log\\.([0-9a-fA-F]+)\\.compacted$");

    private final LedgerDirsManager ledgerDirsManager;
    private final Slogger slog;
    private int nextId;
    private int maxId;

    public EntryLogIdsImpl(LedgerDirsManager ledgerDirsManager,
                           Slogger slog) throws IOException {
        this.ledgerDirsManager = ledgerDirsManager;
        this.slog = slog;
        findLargestGap();
    }

    @Override
    public int nextId() throws IOException {
        while (true) {
            synchronized (this) {
                int current = nextId;
                nextId++;
                if (nextId == maxId) {
                    findLargestGap();
                } else {
                    return current;
                }
            }
        }
    }

    private void findLargestGap() throws IOException {
        long start = System.nanoTime();
        List<Integer> currentIds = new ArrayList<Integer>();

        for (File ledgerDir : ledgerDirsManager.getAllLedgerDirs()) {
            currentIds.addAll(logIdsInDirectory(ledgerDir));
            currentIds.addAll(compactedLogIdsInDirectory(ledgerDir));
        }

        Pair<Integer, Integer> gap = findLargestGap(currentIds);
        nextId = gap.getLeft();
        maxId = gap.getRight();
        slog.kv("dirs", ledgerDirsManager.getAllLedgerDirs())
            .kv("nextId", nextId)
            .kv("maxId", maxId)
            .kv("durationMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
            .info(Events.ENTRYLOG_IDS_CANDIDATES_SELECTED);
    }

    /**
     * O(nlogn) algorithm to find largest contiguous gap between
     * integers in a passed list. n should be relatively small.
     * Entry logs should be about 1GB in size, so even if the node
     * stores a PB, there should be only 1000000 entry logs.
     */
    static Pair<Integer, Integer> findLargestGap(List<Integer> currentIds) {
        if (currentIds.isEmpty()) {
            return Pair.of(0, Integer.MAX_VALUE);
        }

        Collections.sort(currentIds);

        int nextIdCandidate = 0;
        int maxIdCandidate = currentIds.get(0);
        int maxGap = maxIdCandidate - nextIdCandidate;
        for (int i = 0; i < currentIds.size(); i++) {
            int gapStart = currentIds.get(i) + 1;
            int j = i + 1;
            int gapEnd = Integer.MAX_VALUE;
            if (j < currentIds.size()) {
                gapEnd = currentIds.get(j);
            }
            int gapSize = gapEnd - gapStart;
            if (gapSize > maxGap) {
                maxGap = gapSize;
                nextIdCandidate = gapStart;
                maxIdCandidate = gapEnd;
            }
        }
        return Pair.of(nextIdCandidate, maxIdCandidate);
    }

    public static List<Integer> logIdsInDirectory(File directory) {
        List<Integer> ids = new ArrayList<>();
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null && files.length > 0) {
                for (File f : files) {
                    Matcher m = FILE_PATTERN.matcher(f.getName());
                    if (m.matches()) {
                        int logId = Integer.parseUnsignedInt(m.group(1), 16);
                        ids.add(logId);
                    }
                }
            }
        }
        return ids;
    }

    private static List<Integer> compactedLogIdsInDirectory(File directory) {
        List<Integer> ids = new ArrayList<>();
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null && files.length > 0) {
                for (File f : files) {
                    Matcher m = COMPACTED_FILE_PATTERN.matcher(f.getName());
                    if (m.matches()) {
                        int logId = Integer.parseUnsignedInt(m.group(1), 16);
                        ids.add(logId);
                    }
                }
            }
        }
        return ids;

    }
}
