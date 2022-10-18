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
package org.apache.bookkeeper.meta;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.net.BookieId;
import org.apache.zookeeper.KeeperException;

public interface MigrationManager extends AutoCloseable {

    /**
     * Submit the ledgers to be migrated.
     * */
    default void submitToMigrateReplicas(Map<Long, Set<BookieId>> toMigratedLedgerAndBookieMap)
            throws InterruptedException, KeeperException {
        throw new UnsupportedOperationException("submitToMigrateReplicas is not supported "
                + "by this metadata driver");
    }

    /**
     * Get the list of ledgers corresponding to the replica to be migrated.
     * */
    default List<String> listLedgersOfMigrationReplicas()
            throws InterruptedException, KeeperException {
        throw  new UnsupportedOperationException("lockMigrationReplicas is not supported "
                + "by this metadata driver");
    }

    /**
     * After obtaining a replica migration task,
     * lock the replica task to prevent it from being executed by other workers.
     * */
    default void lockMigrationReplicas(long ledgerId, String advertisedAddress)
            throws InterruptedException, KeeperException, UnsupportedOperationException{
        throw  new UnsupportedOperationException("lockMigrationReplicas is not supported "
                + "by this metadata driver");
    }

    /**
     * Get the bookies corresponding to the replica to be migrated.
     * */
    default String getOwnerBookiesMigrationReplicas(long ledgerId)
            throws InterruptedException, KeeperException, UnsupportedEncodingException {
        throw  new UnsupportedOperationException("getOwnerBookiesMigrationReplicas is not supported "
                + "by this metadata driver");
    }

    /**
     * Delete the corresponding ledger path.
     * */
    default void deleteMigrationLedgerPath(long ledgerId)
            throws InterruptedException, KeeperException, UnsupportedOperationException {
        throw  new UnsupportedOperationException("deleteMigrationLedgerPath is not supported "
                + "by this metadata driver");
    }

    /**
     * Delete the corresponding zookeeper path.
     * */
    default void deleteZkPath(String path)
            throws InterruptedException, KeeperException, UnsupportedOperationException {
        throw  new UnsupportedOperationException("deleteZkPath is not supported "
                + "by this metadata driver");
    }

    /**
     * release the lock of a ledger.
     * */
    default void releaseLock(long ledgerId) {
        throw  new UnsupportedOperationException("releaseLock is not supported "
                + "by this metadata driver");
    }


    /**
     * Check if zookeeper path exists.
     * */
    default boolean exists(long ledgerId) throws InterruptedException, KeeperException {
        throw  new UnsupportedOperationException("existPath is not supported "
                + "by this metadata driver");
    }

    /**
     * Release all resources held by the ledger migration manager.
     */
    @Override
    void close();
}
