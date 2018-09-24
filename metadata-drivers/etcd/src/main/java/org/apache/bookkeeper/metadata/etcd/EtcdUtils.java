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
package org.apache.bookkeeper.metadata.etcd;

import static org.apache.bookkeeper.metadata.etcd.EtcdConstants.BUCKETS_NODE;
import static org.apache.bookkeeper.metadata.etcd.EtcdConstants.COOKIES_NODE;
import static org.apache.bookkeeper.metadata.etcd.EtcdConstants.END_SEP;
import static org.apache.bookkeeper.metadata.etcd.EtcdConstants.INSTANCEID_NODE;
import static org.apache.bookkeeper.metadata.etcd.EtcdConstants.LAYOUT_NODE;
import static org.apache.bookkeeper.metadata.etcd.EtcdConstants.LEDGERS_NODE;
import static org.apache.bookkeeper.metadata.etcd.EtcdConstants.MEMBERS_NODE;
import static org.apache.bookkeeper.metadata.etcd.EtcdConstants.READONLY_NODE;
import static org.apache.bookkeeper.metadata.etcd.EtcdConstants.UR_NODE;
import static org.apache.bookkeeper.metadata.etcd.EtcdConstants.WRITEABLE_NODE;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.bookkeeper.bookie.BookieException.MetadataStoreException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Utils for etcd based metadata store.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class EtcdUtils {

    static String getScopeEndKey(String scope) {
        return String.format("%s%s", scope, END_SEP);
    }

    static String getBucketsPath(String scope) {
        return String.format("%s/%s", scope, BUCKETS_NODE);
    }

    static String getBucketPath(String scope, int bucket) {
        return String.format("%s/%s/%03d",
            scope,
            BUCKETS_NODE,
            bucket);
    }

    static String getLayoutKey(String scope) {
        return String.format("%s/%s", scope, LAYOUT_NODE);
    }

    static String getLedgersPath(String scope) {
        return String.format("%s/%s", scope, LEDGERS_NODE);
    }

    static String getLedgerKey(String scope, long ledgerId) {
        return getLedgerKey(scope, 0, ledgerId);
    }

    static String getLedgerKey(String scope, long scopeId, long ledgerId) {
        UUID uuid = new UUID(scopeId, ledgerId);
        return String.format("%s/ledgers/%s", scope, uuid.toString());
    }

    static UUID parseLedgerKey(String ledgerKey) {
        String[] keyParts = StringUtils.split(ledgerKey, '/');
        return UUID.fromString(keyParts[keyParts.length - 1]);
    }

    static String getBookiesPath(String scope) {
        return String.format("%s/%s", scope, MEMBERS_NODE);
    }

    static String getBookiesEndPath(String scope) {
        return String.format("%s/%s%s", scope, MEMBERS_NODE, END_SEP);
    }

    static String getWritableBookiesPath(String scope) {
        return String.format("%s/%s/%s", scope, MEMBERS_NODE, WRITEABLE_NODE);
    }

    static String getWritableBookiesBeginPath(String scope) {
        return String.format("%s/%s/%s/", scope, MEMBERS_NODE, WRITEABLE_NODE);
    }

    static String getWritableBookiesEndPath(String scope) {
        return String.format("%s/%s/%s%s", scope, MEMBERS_NODE, WRITEABLE_NODE, END_SEP);
    }

    static String getWritableBookiePath(String scope, String bookieId) {
        return String.format("%s/%s/%s/%s",
            scope, MEMBERS_NODE, WRITEABLE_NODE, bookieId);
    }

    static String getReadonlyBookiesPath(String scope) {
        return String.format("%s/%s/%s", scope, MEMBERS_NODE, READONLY_NODE);
    }

    static String getReadonlyBookiesBeginPath(String scope) {
        return String.format("%s/%s/%s/", scope, MEMBERS_NODE, READONLY_NODE);
    }

    static String getReadonlyBookiesEndPath(String scope) {
        return String.format("%s/%s/%s%s", scope, MEMBERS_NODE, READONLY_NODE, END_SEP);
    }

    static String getReadonlyBookiePath(String scope, String bookieId) {
        return String.format("%s/%s/%s/%s",
            scope, MEMBERS_NODE, READONLY_NODE, bookieId);
    }

    static String getCookiesPath(String scope) {
        return String.format("%s/%s", scope, COOKIES_NODE);
    }

    static String getCookiePath(String scope, String bookieId) {
        return String.format("%s/%s/%s", scope, COOKIES_NODE, bookieId);
    }

    static String getClusterInstanceIdPath(String scope) {
        return String.format("%s/%s", scope, INSTANCEID_NODE);
    }

    static String getUnderreplicationPath(String scope) {
        return String.format("%s/%s", scope, UR_NODE);
    }

    static <T> T ioResult(CompletableFuture<T> future) throws IOException {
        return FutureUtils.result(future, cause -> {
            if (cause instanceof IOException) {
                return (IOException) cause;
            } else {
                return new IOException(cause);
            }
        });
    }

    static <T> T msResult(CompletableFuture<T> future) throws MetadataStoreException {
        return FutureUtils.result(future, cause -> {
            if (cause instanceof MetadataStoreException) {
                return (MetadataStoreException) cause;
            } else {
                return new MetadataStoreException(cause);
            }
        });
    }

    static <T> T msResult(CompletableFuture<T> future,
                          long timeout,
                          TimeUnit timeUnit)
            throws MetadataStoreException, TimeoutException {
        return FutureUtils.result(future, cause -> {
            if (cause instanceof MetadataStoreException) {
                return (MetadataStoreException) cause;
            } else {
                return new MetadataStoreException(cause);
            }
        }, timeout, timeUnit);
    }

    public static long toLong(byte[] memory, int index) {
        return ((long) memory[index] & 0xff) << 56
            | ((long) memory[index + 1] & 0xff) << 48
            | ((long) memory[index + 2] & 0xff) << 40
            | ((long) memory[index + 3] & 0xff) << 32
            | ((long) memory[index + 4] & 0xff) << 24
            | ((long) memory[index + 5] & 0xff) << 16
            | ((long) memory[index + 6] & 0xff) << 8
            | (long) memory[index + 7] & 0xff;
    }

    /**
     * Convert a long number to a bytes array.
     *
     * @param value the long number
     * @return the bytes array
     */
    public static byte[] toBytes(long value) {
        byte[] memory = new byte[8];
        toBytes(value, memory, 0);
        return memory;
    }

    public static void toBytes(long value, byte[] memory, int index) {
        memory[index] = (byte) (value >>> 56);
        memory[index + 1] = (byte) (value >>> 48);
        memory[index + 2] = (byte) (value >>> 40);
        memory[index + 3] = (byte) (value >>> 32);
        memory[index + 4] = (byte) (value >>> 24);
        memory[index + 5] = (byte) (value >>> 16);
        memory[index + 6] = (byte) (value >>> 8);
        memory[index + 7] = (byte) value;
    }

}
