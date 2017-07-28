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
package org.apache.bookkeeper.client;

import java.lang.Exception;

/**
 * Class the enumerates all the possible error conditions
 *
 */

@SuppressWarnings("serial")
public abstract class BKException extends Exception {

    private int code;

    BKException(int code) {
        this.code = code;
    }

    /**
     * Create an exception from an error code
     * @param code return error code
     * @return corresponding exception
     */
    public static BKException create(int code) {
        switch (code) {
        case Code.ReadException:
            return new BKReadException();
        case Code.QuorumException:
            return new BKQuorumException();
        case Code.NoBookieAvailableException:
            return new BKBookieException();
        case Code.DigestNotInitializedException:
            return new BKDigestNotInitializedException();
        case Code.DigestMatchException:
            return new BKDigestMatchException();
        case Code.NotEnoughBookiesException:
            return new BKNotEnoughBookiesException();
        case Code.NoSuchLedgerExistsException:
            return new BKNoSuchLedgerExistsException();
        case Code.BookieHandleNotAvailableException:
            return new BKBookieHandleNotAvailableException();
        case Code.ZKException:
            return new ZKException();
        case Code.MetaStoreException:
            return new MetaStoreException();
        case Code.LedgerRecoveryException:
            return new BKLedgerRecoveryException();
        case Code.LedgerClosedException:
            return new BKLedgerClosedException();
        case Code.WriteException:
            return new BKWriteException();
        case Code.NoSuchEntryException:
            return new BKNoSuchEntryException();
        case Code.IncorrectParameterException:
            return new BKIncorrectParameterException();
        case Code.InterruptedException:
            return new BKInterruptedException();
        case Code.ProtocolVersionException:
            return new BKProtocolVersionException();
        case Code.MetadataVersionException:
            return new BKMetadataVersionException();
        case Code.LedgerFencedException:
            return new BKLedgerFencedException();
        case Code.UnauthorizedAccessException:
            return new BKUnauthorizedAccessException();
        case Code.UnclosedFragmentException:
            return new BKUnclosedFragmentException();
        case Code.WriteOnReadOnlyBookieException:
            return new BKWriteOnReadOnlyBookieException();
        case Code.ReplicationException:
            return new BKReplicationException();
        case Code.ClientClosedException:
            return new BKClientClosedException();
        case Code.LedgerExistException:
            return new BKLedgerExistException();
        case Code.IllegalOpException:
            return new BKIllegalOpException();
        case Code.AddEntryQuorumTimeoutException:
            return new BKAddEntryQuorumTimeoutException();
        case Code.DuplicateEntryIdException:
            return new BKDuplicateEntryIdException();
        case Code.TimeoutException:
            return new BKTimeoutException();
        case Code.LedgerIdOverflowException:
            return new BKLedgerIdOverflowException();
        default:
            return new BKUnexpectedConditionException();
        }
    }

    /**
     * Codes which represent the various {@link BKException} types.
     */
    public interface Code {
        /** A placer holder (unused) */
        int UNINITIALIZED = 1;
        /** Everything is OK */
        int OK = 0;
        /** Read operations failed (bookie error) */
        int ReadException = -1;
        /** Unused */
        int QuorumException = -2;
        /** Unused */
        int NoBookieAvailableException = -3;
        /** Digest Manager is not initialized (client error) */
        int DigestNotInitializedException = -4;
        /** Digest doesn't match on returned entries */
        int DigestMatchException = -5;
        /** Not enough bookies available to form an ensemble */
        int NotEnoughBookiesException = -6;
        /** No such ledger exists */
        int NoSuchLedgerExistsException = -7;
        /** Bookies are not available */
        int BookieHandleNotAvailableException = -8;
        /** ZooKeeper operations failed */
        int ZKException = -9;
        /** Ledger recovery operations failed */
        int LedgerRecoveryException = -10;
        /** Executing operations on a closed ledger handle */
        int LedgerClosedException = -11;
        /** Write operations failed (bookie error) */
        int WriteException = -12;
        /** No such entry exists */
        int NoSuchEntryException = -13;
        /** Incorrect parameters (operations are absolutely not executed) */
        int IncorrectParameterException = -14;
        /** Synchronous operations are interrupted */
        int InterruptedException = -15;
        /** Protocol version is wrong (operations are absolutely not executed) */
        int ProtocolVersionException = -16;
        /** Bad version on executing metadata operations */
        int MetadataVersionException = -17;
        /** Meta store operations failed */
        int MetaStoreException = -18;
        /** Executing operations on a closed client */
        int ClientClosedException = -19;
        /** Ledger already exists */
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
         * Write operations failed due to bookies are readonly
         */
        int WriteOnReadOnlyBookieException = -104;
        //-105 reserved for TooManyRequestsException
        /**
         * Ledger id overflow happens on ledger manager.
         *
         * @since 4.5
         */
        int LedgerIdOverflowException = -106;

        /**
         * generic exception code used to propagate in replication pipeline
         */
        int ReplicationException = -200;

        /**
         * Unexpected condition.
         */
        int UnexpectedConditionException = -999;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }

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
        default:
            return "Unexpected condition";
        }
    }

    public static class BKReadException extends BKException {
        public BKReadException() {
            super(Code.ReadException);
        }
    }

    public static class BKNoSuchEntryException extends BKException {
        public BKNoSuchEntryException() {
            super(Code.NoSuchEntryException);
        }
    }

    public static class BKQuorumException extends BKException {
        public BKQuorumException() {
            super(Code.QuorumException);
        }
    }

    public static class BKBookieException extends BKException {
        public BKBookieException() {
            super(Code.NoBookieAvailableException);
        }
    }

    public static class BKDigestNotInitializedException extends BKException {
        public BKDigestNotInitializedException() {
            super(Code.DigestNotInitializedException);
        }
    }

    public static class BKDigestMatchException extends BKException {
        public BKDigestMatchException() {
            super(Code.DigestMatchException);
        }
    }

    public static class BKIllegalOpException extends BKException {
        public BKIllegalOpException() {
            super(Code.IllegalOpException);
        }
    }

    public static class BKAddEntryQuorumTimeoutException extends BKException {
        public BKAddEntryQuorumTimeoutException() {
            super(Code.AddEntryQuorumTimeoutException);
        }
    }

    public static class BKDuplicateEntryIdException extends BKException {
        public BKDuplicateEntryIdException() {
            super(Code.DuplicateEntryIdException);
        }
    }

    public static class BKUnexpectedConditionException extends BKException {
        public BKUnexpectedConditionException() {
            super(Code.UnexpectedConditionException);
        }
    }

    public static class BKNotEnoughBookiesException extends BKException {
        public BKNotEnoughBookiesException() {
            super(Code.NotEnoughBookiesException);
        }
    }

    public static class BKWriteException extends BKException {
        public BKWriteException() {
            super(Code.WriteException);
        }
    }

    public static class BKProtocolVersionException extends BKException {
        public BKProtocolVersionException() {
            super(Code.ProtocolVersionException);
        }
    }

    public static class BKMetadataVersionException extends BKException {
        public BKMetadataVersionException() {
            super(Code.MetadataVersionException);
        }
    }

    public static class BKNoSuchLedgerExistsException extends BKException {
        public BKNoSuchLedgerExistsException() {
            super(Code.NoSuchLedgerExistsException);
        }
    }

    public static class BKBookieHandleNotAvailableException extends BKException {
        public BKBookieHandleNotAvailableException() {
            super(Code.BookieHandleNotAvailableException);
        }
    }

    public static class ZKException extends BKException {
        public ZKException() {
            super(Code.ZKException);
        }
    }

    public static class MetaStoreException extends BKException {
        public MetaStoreException() {
            super(Code.MetaStoreException);
        }
    }

    public static class BKLedgerExistException extends BKException {
        public BKLedgerExistException() {
            super(Code.LedgerExistException);
        }
    }

    public static class BKLedgerRecoveryException extends BKException {
        public BKLedgerRecoveryException() {
            super(Code.LedgerRecoveryException);
        }
    }

    public static class BKLedgerClosedException extends BKException {
        public BKLedgerClosedException() {
            super(Code.LedgerClosedException);
        }
    }

    public static class BKIncorrectParameterException extends BKException {
        public BKIncorrectParameterException() {
            super(Code.IncorrectParameterException);
        }
    }

    public static class BKInterruptedException extends BKException {
        public BKInterruptedException() {
            super(Code.InterruptedException);
        }
    }

    public static class BKLedgerFencedException extends BKException {
        public BKLedgerFencedException() {
            super(Code.LedgerFencedException);
        }
    }

    public static class BKUnauthorizedAccessException extends BKException {
        public BKUnauthorizedAccessException() {
            super(Code.UnauthorizedAccessException);
        }
    }

    public static class BKUnclosedFragmentException extends BKException {
        public BKUnclosedFragmentException() {
            super(Code.UnclosedFragmentException);
        }
    }

    public static class BKWriteOnReadOnlyBookieException extends BKException {
        public BKWriteOnReadOnlyBookieException() {
            super(Code.WriteOnReadOnlyBookieException);
        }
    }

    public static class BKReplicationException extends BKException {
        public BKReplicationException() {
            super(Code.ReplicationException);
        }
    }

    public static class BKClientClosedException extends BKException {
        public BKClientClosedException() {
            super(Code.ClientClosedException);
        }
    }

    public static class BKTimeoutException extends BKException {
        public BKTimeoutException() {
            super(Code.TimeoutException);
        }
    }

    public static class BKLedgerIdOverflowException extends BKException {
        public BKLedgerIdOverflowException() {
            super(Code.LedgerIdOverflowException);
        }
    }
}
