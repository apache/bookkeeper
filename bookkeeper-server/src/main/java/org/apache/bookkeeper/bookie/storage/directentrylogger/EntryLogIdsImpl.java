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
package org.apache.bookkeeper.bookie.storage.directentrylogger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.EntryLogIds;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.util.LedgerDirUtil;
import org.apache.commons.lang3.tuple.Pair;
/**
 * EntryLogIdsImpl.
 */
public class EntryLogIdsImpl implements EntryLogIds {


    private final LedgerDirsManager ledgerDirsManager;
    private final Slogger slog;
    private int nextId;
    private int maxId;

    public EntryLogIdsImpl(LedgerDirsManager ledgerDirsManager,
                           Slogger slog) throws IOException {
        this.ledgerDirsManager = ledgerDirsManager;
        this.slog = slog.ctx(EntryLogIdsImpl.class);
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
            currentIds.addAll(LedgerDirUtil.logIdsInDirectory(ledgerDir));
            currentIds.addAll(LedgerDirUtil.compactedLogIdsInDirectory(ledgerDir));
        }
        Pair<Integer, Integer> gap = LedgerDirUtil.findLargestGap(currentIds);
        nextId = gap.getLeft();
        maxId = gap.getRight();
        slog.kv("dirs", ledgerDirsManager.getAllLedgerDirs())
            .kv("nextId", nextId)
            .kv("maxId", maxId)
            .kv("durationMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
            .info(Events.ENTRYLOG_IDS_CANDIDATES_SELECTED);
    }
}
