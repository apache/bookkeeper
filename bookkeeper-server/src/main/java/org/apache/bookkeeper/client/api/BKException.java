/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client.api;

import org.apache.bookkeeper.client.LedgerHandleAdv;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;

/**
 * Super class for all errors which occur using BookKeeper client.
 *
 * @since 4.6
 */
@Public
@Unstable
public abstract class BKException extends Exception {
    protected final int code;

    /**
     * Create a new exception.
     *
     * @param code the error code
     *
     * @see Code
     */
    public BKException(int code) {
        super(getMessage(code));
        this.code = code;
    }

    /**
     * Get the return code for the exception.
     *
     * @return the error code
     *
     * @see Code
     */
    public final int getCode() {
        return this.code;
    }

    /**
     * Describe an error code.
     *
     * @param code
     *
     * @return the description of the error code
     */
    public static String getMessage(int code) {
        switch (code) {
        case Code.OK:
            return "No problem";
        case Code.ReadException:
            return "Error while reading ledger";
        case Code.QuorumException:
            return "Invalid quorum size on ensemble size";
        case Code.NoBookieAvailableException:
            return "Invalid quorum size on ensemble size";
        case Code.DigestNotInitializedException:
            return "Digest engine not initialized";
        case Code.DigestMatchException:
            return "Entry digest does not match";
        case Code.NotEnoughBookiesException:
            return "Not enough non-faulty bookies available";
        case Code.NoSuchLedgerExistsException:
            return "No such ledger exists";
        case Code.BookieHandleNotAvailableException:
            return "Bookie handle is not available";
        case Code.ZKException:
            return "Error while using ZooKeeper";
        case Code.MetaStoreException:
            return "Error while using MetaStore";
        case Code.LedgerExistException:
            return "Ledger existed";
        case Code.LedgerRecoveryException:
            return "Error while recovering ledger";
        case Code.LedgerClosedException:
            return "Attempt to write to a closed ledger";
        case Code.WriteException:
            return "Write failed on bookie";
        case Code.NoSuchEntryException:
            return "No such entry";
        case Code.IncorrectParameterException:
            return "Incorrect parameter input";
        case Code.InterruptedException:
            return "Interrupted while waiting for permit";
        case Code.ProtocolVersionException:
            return "Bookie protocol version on server is incompatible with client";
        case Code.MetadataVersionException:
            return "Bad ledger metadata version";
        case Code.DuplicateEntryIdException:
            return "Attempted to add Duplicate entryId";
        case Code.LedgerFencedException:
            return "Ledger has been fenced off. Some other client must have opened it to read";
        case Code.UnauthorizedAccessException:
            return "Attempted to access ledger using the wrong password";
        case Code.UnclosedFragmentException:
            return "Attempting to use an unclosed fragment; This is not safe";
        case Code.WriteOnReadOnlyBookieException:
            return "Attempting to write on ReadOnly bookie";
        case Code.LedgerIdOverflowException:
            return "Next ledgerID is too large.";
        case Code.ReplicationException:
            return "Errors in replication pipeline";
        case Code.ClientClosedException:
            return "BookKeeper client is closed";
        case Code.IllegalOpException:
            return "Invalid operation";
        case Code.AddEntryQuorumTimeoutException:
            return "Add entry quorum wait timed out";
        case Code.TimeoutException:
            return "Bookie operation timeout";
        case Code.SecurityException:
            return "Failed to establish a secure connection";
        default:
            return "Unexpected condition";
        }
    }

    /**
     * Codes which represent the various exceptoin types.
     */
    public interface Code {
        /** A placer holder (unused). */
        int UNINITIALIZED = 1;
        /** Everything is OK. */
        int OK = 0;
        /** Read operations failed (bookie error). */
        int ReadException = -1;
        /** Unused. */
        int QuorumException = -2;
        /** Unused. */
        int NoBookieAvailableException = -3;
        /** Digest Manager is not initialized (client error). */
        int DigestNotInitializedException = -4;
        /** Digest doesn't match on returned entries. */
        int DigestMatchException = -5;
        /** Not enough bookies available to form an ensemble. */
        int NotEnoughBookiesException = -6;
        /** No such ledger exists. */
        int NoSuchLedgerExistsException = -7;
        /** Bookies are not available. */
        int BookieHandleNotAvailableException = -8;
        /** ZooKeeper operations failed. */
        int ZKException = -9;
        /** Ledger recovery operations failed. */
        int LedgerRecoveryException = -10;
        /** Executing operations on a closed ledger handle. */
        int LedgerClosedException = -11;
        /** Write operations failed (bookie error). */
        int WriteException = -12;
        /** No such entry exists. */
        int NoSuchEntryException = -13;
        /** Incorrect parameters (operations are absolutely not executed). */
        int IncorrectParameterException = -14;
        /** Synchronous operations are interrupted. */
        int InterruptedException = -15;
        /** Protocol version is wrong (operations are absolutely not executed). */
        int ProtocolVersionException = -16;
        /** Bad version on executing metadata operations. */
        int MetadataVersionException = -17;
        /** Meta store operations failed. */
        int MetaStoreException = -18;
        /** Executing operations on a closed client. */
        int ClientClosedException = -19;
        /** Ledger already exists. */
        int LedgerExistException = -20;
        /**
         * Add entry operation timeouts on waiting quorum responses.
         *
         * @since 4.5
         */
        int AddEntryQuorumTimeoutException = -21;
        /**
         * Duplicated entry id is found when {@link LedgerHandleAdv#addEntry(long, byte[])}.
         *
         * @since 4.5
         */
        int DuplicateEntryIdException = -22;
        /**
         * Operations timeouts.
         *
         * @since 4.5
         */
        int TimeoutException = -23;
        int SecurityException = -24;

        /**
         * Operation is illegal.
         */
        int IllegalOpException = -100;
        /**
         * Operations failed due to ledgers are fenced.
         */
        int LedgerFencedException = -101;
        /**
         * Operations failed due to unauthorized.
         */
        int UnauthorizedAccessException = -102;
        /**
         * Replication failed due to unclosed fragments.
         */
        int UnclosedFragmentException = -103;
        /**
         * Write operations failed due to bookies are readonly.
         */
        int WriteOnReadOnlyBookieException = -104;
        /**
         * Operations failed due to too many requests in the queue.
         */
        int TooManyRequestsException = -105;
        /**
         * Ledger id overflow happens on ledger manager.
         *
         * @since 4.5
         */
        int LedgerIdOverflowException = -106;

        /**
         * Generic exception code used to propagate in replication pipeline.
         */
        int ReplicationException = -200;

        /**
         * Unexpected condition.
         */
        int UnexpectedConditionException = -999;
    }

}
