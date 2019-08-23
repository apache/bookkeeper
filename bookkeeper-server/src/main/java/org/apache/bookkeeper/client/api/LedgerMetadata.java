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

package org.apache.bookkeeper.client.api;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.LimitedPrivate;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;
import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * Represents the client-side metadata of a ledger. It is immutable.
 *
 * @since 4.6
 */
@LimitedPrivate
@Unstable
public interface LedgerMetadata {

    /**
     * Returns the ensemble size of this ledger.
     *
     * @return the ensemble size of this ledger.
     */
    int getEnsembleSize();

    /**
     * Returns the write quorum size of this ledger.
     *
     * @return the write quorum size of this ledger.
     */
    int getWriteQuorumSize();

    /**
     * Returns the ack quorum size of this ledger.
     *
     * @return the ack quorum size of this ledger.
     */
    int getAckQuorumSize();

    /**
     * Returns the last entry id of this ledger.
     *
     * <p>If this ledger is not sealed {@link #isClosed()}, it returns {@code -1L}.
     *
     * @return the last entry id of this ledger if it is sealed, otherwise -1.
     */
    long getLastEntryId();

    /**
     * Returns the length of this ledger.
     *
     * <p>If this ledger is not sealed {@link #isClosed()}, it returns {@code 0}.
     *
     * @return the length of this ledger if it is sealed, otherwise 0.
     */
    long getLength();

    /**
     * Whether the metadata contains the password and digest type for the ledger.
     * Ledgers created with version 4.1.0 clients or older do not have this information.
     *
     * @return true if the metadata contains the password and digest type, false otherwise.
     */
    boolean hasPassword();

    /**
     * Get the password for the ledger.
     * For ledgers created with version 4.1.0 or older, an empty byte array is returned.
     *
     * @return the password for the ledger.
     */
    byte[] getPassword();

    /**
     * Returns the digest type used by this ledger.
     * May return null if the ledger was created with version 4.1.0 or below.
     *
     * @return the digest type used by this ledger.
     */
    DigestType getDigestType();

    /**
     * Returns the creation timestamp of this ledger.
     *
     * @return the creation timestamp of this ledger.
     */
    long getCtime();

    /**
     * Returns whether the ledger is sealed or not.
     *
     * @return true if the ledger is sealed, otherwise false.
     */
    boolean isClosed();

    /**
     * Returns the custom metadata stored with the ledgers.
     *
     * @return the custom metadata stored with the ledgers.
     */
    Map<String, byte[]> getCustomMetadata();

    /**
     * Returns the ensemble at the given {@code entryId}.
     *
     * @param entryId the entry id to retrieve its ensemble information
     * @return the ensemble which contains the given {@code entryId}.
     */
    List<BookieSocketAddress> getEnsembleAt(long entryId);

    /**
     * Returns all the ensembles of this entry.
     *
     * @return all the ensembles of this entry.
     */
    NavigableMap<Long, ? extends List<BookieSocketAddress>> getAllEnsembles();

    /**
     * Returns the state of the metadata.
     *
     * @return the state of the metadata.
     */
    State getState();

    /**
     * Possible metadata states.
     */
    enum State {
        /** The ledger is open. New entry may be added to it. */
        OPEN,

        /** A reader has tried to, or may be trying to recover the ledger.
            The writer may be able to add new entries if fencing hasn't already occurred,
            but any attempt to change ensemble will fail and the write will be forced to
            close the ledger.
        */
        IN_RECOVERY,

        /** The ledger is closed. No new entries may be added to it.
            The length and lastEntryId are fixed. Ensembles may change, but only for rereplication.
        */
        CLOSED
    }

    /**
     * Similar to #toString(), but omits the password of the ledger, so that it is safe to log the output.
     *
     * @return a string representation of the metadata, omitting the password.
     */
    String toSafeString();

    /**
     * Get the format version which should be used to serialize the metadata.
     *
     * @return the format version.
     */
    int getMetadataFormatVersion();

    /**
     * Get the unique creator token of the Ledger.
     *
     * @return the creator token
     */
    long getCToken();
}
