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

/**
 * Class the enumerates all the possible error conditions.
 *
 * <P>This class is going to be deprecate soon, please use the new class {@link BKException}
 */
@SuppressWarnings("serial")
public abstract class BKException extends org.apache.bookkeeper.client.api.BKException {

    BKException(int code) {
        super(code);
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
        case Code.TooManyRequestsException:
            return new BKTooManyRequestsException();
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
        case Code.SecurityException:
            return new BKSecurityException();
        default:
            return new BKUnexpectedConditionException();
        }
    }

    /**
     * Legacy interface which holds constants for BookKeeper error codes.
     * The list has been moved to {@link BKException}
     */
    public interface Code extends org.apache.bookkeeper.client.api.BKException.Code {
    }

    public static class BKSecurityException extends BKException {
        public BKSecurityException() {
            super(Code.SecurityException);
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

    public static class BKTooManyRequestsException extends BKException {
        public BKTooManyRequestsException() {
            super(Code.TooManyRequestsException);
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
