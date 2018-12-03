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
package org.apache.bookkeeper.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for working with ledger metadata.
 */
public class LedgerMetadataUtils {
    static final Logger LOG = LoggerFactory.getLogger(LedgerMetadataUtils.class);

    static List<BookieSocketAddress> getCurrentEnsemble(LedgerMetadata metadata) {
        return getLastEnsembleValue(metadata);
    }

    /**
     * the entry id greater than the given entry-id at which the next ensemble change takes
     * place.
     *
     * @param entryId
     * @return the entry id of the next ensemble change (-1 if no further ensemble changes)
     */
    static long getNextEnsembleChange(LedgerMetadata metadata, long entryId) {
        SortedMap<Long, ? extends List<BookieSocketAddress>> tailMap = metadata.getAllEnsembles().tailMap(entryId + 1);

        if (tailMap.isEmpty()) {
            return -1;
        } else {
            return tailMap.firstKey();
        }
    }

    static Set<BookieSocketAddress> getBookiesInThisLedger(LedgerMetadata metadata) {
        Set<BookieSocketAddress> bookies = new HashSet<BookieSocketAddress>();
        for (List<BookieSocketAddress> ensemble : metadata.getAllEnsembles().values()) {
            bookies.addAll(ensemble);
        }
        return bookies;
    }

    static List<BookieSocketAddress> getLastEnsembleValue(LedgerMetadata metadata) {
        checkArgument(!metadata.getAllEnsembles().isEmpty(), "Metadata should never be created with no ensembles");
        return metadata.getAllEnsembles().lastEntry().getValue();
    }

    static Long getLastEnsembleKey(LedgerMetadata metadata) {
        checkArgument(!metadata.getAllEnsembles().isEmpty(), "Metadata should never be created with no ensembles");
        return metadata.getAllEnsembles().lastKey();
    }

    public static boolean shouldStoreCtime(LedgerMetadata metadata) {
        return metadata instanceof LedgerMetadataImpl && ((LedgerMetadataImpl) metadata).shouldStoreCtime();
    }
}
