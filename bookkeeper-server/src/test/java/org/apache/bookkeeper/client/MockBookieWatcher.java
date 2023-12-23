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

package org.apache.bookkeeper.client;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.StatsLogger;

public class MockBookieWatcher extends BookieWatcherImpl {

    public MockBookieWatcher(ClientConfiguration conf, EnsemblePlacementPolicy placementPolicy,
                             RegistrationClient registrationClient, BookieAddressResolver bookieAddressResolver,
                             StatsLogger statsLogger) {
        super(conf, placementPolicy, registrationClient, bookieAddressResolver, statsLogger);
    }

    @Override
    public BookieId replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                  Map<String, byte[]> customMetadata, List<BookieId> existingBookies, int bookieIdx,
                                  Set<BookieId> excludeBookies) throws BKException.BKNotEnoughBookiesException {
        throw new BKException.BKNotEnoughBookiesException();
    }
}
