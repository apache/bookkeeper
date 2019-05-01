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

import java.util.function.Function;

/**
 * Class the enumerates all the possible error conditions.
 *
 * <P>This class is going to be deprecate soon, please use the new class {@link BKException}
 */
@SuppressWarnings("serial")
public abstract class BKException extends org.apache.bookkeeper.client.api.BKException {

    public static final Function<Throwable, BKException> HANDLER = cause -> {
        if (cause == null) {
            return null;
        }
        if (cause instanceof BKException) {
            return (BKException) cause;
        } else {
            BKException ex = new BKUnexpectedConditionException();
            ex.initCause(cause);
            return ex;
        }
    };

    BKException(int code) {
        super(code);
    }

    BKException(int code, Throwable cause) {
        super(code, cause);
    }

    /**
     * Create an exception from an error code.
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
        case Code.NoSuchLedgerExistsOnMetadataServerException:
            return new BKNoSuchLedgerExistsOnMetadataServerException();
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
        case Code.MetadataSerializationException:
            return new BKMetadataSerializationException();
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

    /**
     * Bookkeeper security exception.
     */
    public static class BKSecurityException extends BKException {
        public BKSecurityException() {
            super(BKException.Code.SecurityException);
        }
    }

    /**
     * Bookkeeper read exception.
     */
    public static class BKReadException extends BKException {
        public BKReadException() {
            super(BKException.Code.ReadException);
        }
    }

    /**
     * Bookkeeper no such entry exception.
     */
    public static class BKNoSuchEntryException extends BKException {
        public BKNoSuchEntryException() {
            super(BKException.Code.NoSuchEntryException);
        }
    }

    /**
     * Bookkeeper quorum exception.
     */
    public static class BKQuorumException extends BKException {
        public BKQuorumException() {
            super(BKException.Code.QuorumException);
        }
    }

    /**
     * Bookkeeper bookie exception.
     */
    public static class BKBookieException extends BKException {
        public BKBookieException() {
            super(BKException.Code.NoBookieAvailableException);
        }
    }

    /**
     * Bookkeeper digest not initialized exception.
     */
    public static class BKDigestNotInitializedException extends BKException {
        public BKDigestNotInitializedException() {
            super(BKException.Code.DigestNotInitializedException);
        }
    }

    /**
     * Bookkeeper digest match exception.
     */
    public static class BKDigestMatchException extends BKException {
        public BKDigestMatchException() {
            super(BKException.Code.DigestMatchException);
        }
    }

    /**
     * Bookkeeper illegal operation exception.
     */
    public static class BKIllegalOpException extends BKException {
        public BKIllegalOpException() {
            super(BKException.Code.IllegalOpException);
        }
    }

    /**
     * Bookkeeper add entry quorum timeout exception.
     */
    public static class BKAddEntryQuorumTimeoutException extends BKException {
        public BKAddEntryQuorumTimeoutException() {
            super(BKException.Code.AddEntryQuorumTimeoutException);
        }
    }

    /**
     * Bookkeeper duplicate entry id exception.
     */
    public static class BKDuplicateEntryIdException extends BKException {
        public BKDuplicateEntryIdException() {
            super(BKException.Code.DuplicateEntryIdException);
        }
    }

    /**
     * Bookkeeper unexpected condition exception.
     */
    public static class BKUnexpectedConditionException extends BKException {
        public BKUnexpectedConditionException() {
            super(BKException.Code.UnexpectedConditionException);
        }
    }

    /**
     * Bookkeeper not enough bookies exception.
     */
    public static class BKNotEnoughBookiesException extends BKException {
        public BKNotEnoughBookiesException() {
            super(BKException.Code.NotEnoughBookiesException);
        }
    }

    /**
     * Bookkeeper write exception.
     */
    public static class BKWriteException extends BKException {
        public BKWriteException() {
            super(BKException.Code.WriteException);
        }
    }

    /**
     * Bookkeeper protocol version exception.
     */
    public static class BKProtocolVersionException extends BKException {
        public BKProtocolVersionException() {
            super(BKException.Code.ProtocolVersionException);
        }
    }

    /**
     * Bookkeeper metadata version exception.
     */
    public static class BKMetadataVersionException extends BKException {
        public BKMetadataVersionException() {
            super(BKException.Code.MetadataVersionException);
        }
    }

    /**
     * Bookkeeper no such ledger exists exception.
     */
    public static class BKNoSuchLedgerExistsException extends BKException {
        public BKNoSuchLedgerExistsException() {
            super(BKException.Code.NoSuchLedgerExistsException);
        }
    }
    /**
     * Bookkeeper no such ledger exists on metadata server exception.
     */
    public static class BKNoSuchLedgerExistsOnMetadataServerException extends BKException {
        public BKNoSuchLedgerExistsOnMetadataServerException() {
            super(Code.NoSuchLedgerExistsOnMetadataServerException);
        }
    }

    /**
     * Bookkeeper bookie handle not available exception.
     */
    public static class BKBookieHandleNotAvailableException extends BKException {
        public BKBookieHandleNotAvailableException() {
            super(BKException.Code.BookieHandleNotAvailableException);
        }
    }

    /**
     * Zookeeper exception.
     */
    public static class ZKException extends BKException {
        public ZKException() {
            super(BKException.Code.ZKException);
        }
    }

    /**
     * Metastore exception.
     */
    public static class MetaStoreException extends BKException {
        public MetaStoreException() {
            super(BKException.Code.MetaStoreException);
        }

        public MetaStoreException(Throwable cause) {
            super(BKException.Code.MetaStoreException, cause);
        }
    }

    /**
     * Bookkeeper ledger exist exception.
     */
    public static class BKLedgerExistException extends BKException {
        public BKLedgerExistException() {
            super(BKException.Code.LedgerExistException);
        }
    }

    /**
     * Bookkeeper ledger recovery exception.
     */
    public static class BKLedgerRecoveryException extends BKException {
        public BKLedgerRecoveryException() {
            super(BKException.Code.LedgerRecoveryException);
        }
    }

    /**
     * Bookkeeper ledger closed exception.
     */
    public static class BKLedgerClosedException extends BKException {
        public BKLedgerClosedException() {
            super(BKException.Code.LedgerClosedException);
        }
    }

    /**
     * Bookkeeper incorrect parameter exception.
     */
    public static class BKIncorrectParameterException extends BKException {
        public BKIncorrectParameterException() {
            super(BKException.Code.IncorrectParameterException);
        }
    }

    /**
     * Bookkeeper interrupted exception.
     */
    public static class BKInterruptedException extends BKException {
        public BKInterruptedException() {
            super(BKException.Code.InterruptedException);
        }
    }

    /**
     * Bookkeeper ledger fenced exception.
     */
    public static class BKLedgerFencedException extends BKException {
        public BKLedgerFencedException() {
            super(BKException.Code.LedgerFencedException);
        }
    }

    /**
     * Bookkeeper unauthorized access exception.
     */
    public static class BKUnauthorizedAccessException extends BKException {
        public BKUnauthorizedAccessException() {
            super(BKException.Code.UnauthorizedAccessException);
        }
    }

    /**
     * Bookkeeper unclosed fragment exception.
     */
    public static class BKUnclosedFragmentException extends BKException {
        public BKUnclosedFragmentException() {
            super(BKException.Code.UnclosedFragmentException);
        }
    }

    /**
     * Bookkeeper write on readonly bookie exception.
     */
    public static class BKWriteOnReadOnlyBookieException extends BKException {
        public BKWriteOnReadOnlyBookieException() {
            super(BKException.Code.WriteOnReadOnlyBookieException);
        }
    }

    /**
     * Bookkeeper too many requests exception.
     */
    public static class BKTooManyRequestsException extends BKException {
        public BKTooManyRequestsException() {
            super(BKException.Code.TooManyRequestsException);
        }
    }

    /**
     * Bookkeeper replication exception.
     */
    public static class BKReplicationException extends BKException {
        public BKReplicationException() {
            super(BKException.Code.ReplicationException);
        }
    }

    /**
     * Bookkeeper client closed exception.
     */
    public static class BKClientClosedException extends BKException {
        public BKClientClosedException() {
            super(BKException.Code.ClientClosedException);
        }
    }

    /**
     * Bookkeeper timeout exception.
     */
    public static class BKTimeoutException extends BKException {
        public BKTimeoutException() {
            super(BKException.Code.TimeoutException);
        }
    }

    /**
     * Bookkeeper ledger id overflow exception.
     */
    public static class BKLedgerIdOverflowException extends BKException {
        public BKLedgerIdOverflowException() {
            super(BKException.Code.LedgerIdOverflowException);
        }
    }

    /**
     * Bookkeeper metadata serialization exception.
     */
    public static class BKMetadataSerializationException extends BKException {
        public BKMetadataSerializationException() {
            super(BKException.Code.MetadataSerializationException);
        }

        public BKMetadataSerializationException(Throwable cause) {
            super(BKException.Code.MetadataSerializationException, cause);
        }
    }

    /**
     * Extract an exception code from an BKException, or use a default if it's another type.
     * The throwable is null, assume that no exception took place and return
     * {@link BKException.Code.OK}.
     */
    public static int getExceptionCode(Throwable t, int defaultCode) {
        if (t == null) {
            return BKException.Code.OK;
        } else if (t instanceof BKException) {
            return ((BKException) t).getCode();
        } else if (t.getCause() != null) {
            return getExceptionCode(t.getCause(), defaultCode);
        } else {
            return defaultCode;
        }
    }

    /**
     * Extract an exception code from an BKException, or default to unexpected exception if throwable
     * is not a BKException.
     *
     * @see #getExceptionCode(Throwable,int)
     */
    public static int getExceptionCode(Throwable t) {
        return getExceptionCode(t, Code.UnexpectedConditionException);
    }
}
