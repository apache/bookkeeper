package org.apache.bookkeeper.client;

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
     * @return correponding exception
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
        case Code.IllegalOpException:
            return new BKIllegalOpException();
        default:
            return new BKUnexpectedConditionException();
        }
    }

    /**
     * List of return codes
     *
     */
    public interface Code {
        int OK = 0;
        int ReadException = -1;
        int QuorumException = -2;
        int NoBookieAvailableException = -3;
        int DigestNotInitializedException = -4;
        int DigestMatchException = -5;
        int NotEnoughBookiesException = -6;
        int NoSuchLedgerExistsException = -7;
        int BookieHandleNotAvailableException = -8;
        int ZKException = -9;
        int LedgerRecoveryException = -10;
        int LedgerClosedException = -11;
        int WriteException = -12;
        int NoSuchEntryException = -13;
        int IncorrectParameterException = -14;
        int InterruptedException = -15;
        int ProtocolVersionException = -16;
        int MetadataVersionException = -17;
        int MetaStoreException = -18;

        int IllegalOpException = -100;
        int LedgerFencedException = -101;
        int UnauthorizedAccessException = -102;
        int UnclosedFragmentException = -103;
        int WriteOnReadOnlyBookieException = -104;

        // generic exception code used to propagate in replication pipeline
        int ReplicationException = -200;

        // For all unexpected error conditions
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
        case Code.LedgerFencedException:
            return "Ledger has been fenced off. Some other client must have opened it to read";
        case Code.UnauthorizedAccessException:
            return "Attempted to access ledger using the wrong password";
        case Code.UnclosedFragmentException:
            return "Attempting to use an unclosed fragment; This is not safe";
        case Code.WriteOnReadOnlyBookieException:
            return "Attempting to write on ReadOnly bookie";
        case Code.ReplicationException:
            return "Errors in replication pipeline";
        case Code.IllegalOpException:
            return "Invalid operation";
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
}
