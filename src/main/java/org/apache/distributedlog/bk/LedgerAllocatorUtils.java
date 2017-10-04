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
package org.apache.distributedlog.bk;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.distributedlog.BookKeeperClient;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.ZooKeeperClient;



/**
 * LedgerAllocator utilities.
 */
public class LedgerAllocatorUtils {

    /**
     * Create ledger allocator pool.
     *
     * @param poolPath
     *          ledger allocator pool path.
     * @param corePoolSize
     *          ledger allocator pool core size.
     * @param conf
     *          distributedlog configuration.
     * @param zkc
     *          zookeeper client
     * @param bkc
     *          bookkeeper client
     * @return ledger allocator
     * @throws IOException
     */
    public static LedgerAllocator createLedgerAllocatorPool(
            String poolPath,
            int corePoolSize,
            DistributedLogConfiguration conf,
            ZooKeeperClient zkc,
            BookKeeperClient bkc,
            ScheduledExecutorService scheduledExecutorService) throws IOException {
        return new LedgerAllocatorPool(poolPath, corePoolSize, conf, zkc, bkc, scheduledExecutorService);
    }
}
