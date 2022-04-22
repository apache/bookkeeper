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
package org.apache.bookkeeper.bookie;

/**
 * Signals that a Bookie exception of some sort has occurred. This class
 * is the general class of exceptions produced by failed or interrupted bookie operations.
 */
@SuppressWarnings("serial")
public abstract class BookieException extends Exception {

    private final int code;

    public BookieException(int code) {
        super();
        this.code = code;
    }

    public BookieException(int code, Throwable t) {
        super(t);
        this.code = code;
    }

    public BookieException(int code, String reason) {
        super(reason);
        this.code = code;
    }

    public BookieException(int code, String reason, Throwable t) {
        super(reason, t);
        this.code = code;
    }

    public static BookieException create(int code) {
        switch(code) {
        case Code.UnauthorizedAccessException:
            return new BookieUnauthorizedAccessException();
        case Code.LedgerFencedException:
            return new LedgerFencedException();
        case Code.InvalidCookieException:
            return new InvalidCookieException();
        case Code.UpgradeException:
            return new UpgradeException();
        case Code.DiskPartitionDuplicationException:
            return new DiskPartitionDuplicationException();
        case Code.CookieNotFoundException:
            return new CookieNotFoundException();
        case Code.CookieExistsException:
            return new CookieExistException();
        case Code.MetadataStoreException:
            return new MetadataStoreException();
        case Code.UnknownBookieIdException:
            return new UnknownBookieIdException();
        case Code.DataUnknownException:
            return new DataUnknownException();
        default:
            return new BookieIllegalOpException();
        }
    }

    /**
     * An exception code indicates the failure reason.
     */
    public interface Code {
        int OK = 0;
        int UnauthorizedAccessException = -1;

        int IllegalOpException = -100;
        int LedgerFencedException = -101;
        int InvalidCookieException = -102;
        int UpgradeException = -103;
        int DiskPartitionDuplicationException = -104;
        int CookieNotFoundException = -105;
        int MetadataStoreException = -106;
        int UnknownBookieIdException = -107;
        int OperationRejectedException = -108;
        int CookieExistsException = -109;
        int EntryLogMetadataMapException = -110;
        int DataUnknownException = -111;
    }

    public int getCode() {
        return this.code;
    }

    public String getMessage(int code) {
        String err;
        switch(code) {
        case Code.OK:
            err = "No problem";
            break;
        case Code.UnauthorizedAccessException:
            err = "Error while reading ledger";
            break;
        case Code.LedgerFencedException:
            err = "Ledger has been fenced; No more entries can be added";
            break;
        case Code.InvalidCookieException:
            err = "Invalid environment cookie found";
            break;
        case Code.UpgradeException:
            err = "Error performing an upgrade operation ";
            break;
        case Code.DiskPartitionDuplicationException:
            err = "Disk Partition Duplication is not allowed";
            break;
        case Code.CookieNotFoundException:
            err = "Cookie not found";
            break;
        case Code.CookieExistsException:
            err = "Cookie already exists";
            break;
        case Code.EntryLogMetadataMapException:
            err = "Error in accessing Entry-log metadata map";
            break;
        case Code.MetadataStoreException:
            err = "Error performing metadata operations";
            break;
        case Code.UnknownBookieIdException:
            err = "Unknown bookie id";
            break;
        case Code.OperationRejectedException:
            err = "Operation rejected";
            break;
        case Code.DataUnknownException:
            err = "Unable to respond, ledger is in unknown state";
            break;
        default:
            err = "Invalid operation";
            break;
        }
        String reason = super.getMessage();
        if (reason == null) {
            if (super.getCause() != null) {
                reason = super.getCause().getMessage();
            }
        }
        if (reason == null) {
            return err;
        } else {
            return String.format("%s [%s]", err, reason);
        }
    }

    /**
     * Signals that an unauthorized operation attempts to access the data in a bookie.
     */
    public static class BookieUnauthorizedAccessException extends BookieException {
        public BookieUnauthorizedAccessException() {
            super(Code.UnauthorizedAccessException);
        }

        public BookieUnauthorizedAccessException(String reason) {
            super(Code.UnauthorizedAccessException, reason);
        }
    }

    /**
     * Signals that an illegal operation attempts to access the data in a bookie.
     */
    public static class BookieIllegalOpException extends BookieException {
        public BookieIllegalOpException() {
            super(Code.IllegalOpException);
        }

        public BookieIllegalOpException(String reason) {
            super(Code.IllegalOpException, reason);
        }

        public BookieIllegalOpException(Throwable cause) {
            super(Code.IllegalOpException, cause);
        }
    }

    /**
     * Signals that a ledger has been fenced in a bookie. No more entries can be appended to that ledger.
     */
    public static class LedgerFencedException extends BookieException {
        public LedgerFencedException() {
            super(Code.LedgerFencedException);
        }
    }

    /**
     * Signals that a ledger's operation has been rejected by an internal component because of the resource saturation.
     */
    public static class OperationRejectedException extends BookieException {
        public OperationRejectedException() {
            super(Code.OperationRejectedException);
        }

        @Override
        public Throwable fillInStackTrace() {
            // Since this exception is a way to signal a specific condition and it's triggered and very specific points,
            // we can disable stack traces.
            return null;
        }
    }

    /**
     * Signal that an invalid cookie is found when starting a bookie.
     *
     * <p>This exception is mainly used for detecting if there is any malformed configuration in a bookie.
     */
    public static class InvalidCookieException extends BookieException {
        public InvalidCookieException() {
            this("");
        }

        public InvalidCookieException(String reason) {
            super(Code.InvalidCookieException, reason);
        }

        public InvalidCookieException(Throwable cause) {
            super(Code.InvalidCookieException, cause);
        }
    }

    /**
     * Signal that no cookie is found when starting a bookie.
     */
    public static class CookieNotFoundException extends BookieException {
        public CookieNotFoundException() {
            this("");
        }

        public CookieNotFoundException(String reason) {
            super(Code.CookieNotFoundException, reason);
        }

        public CookieNotFoundException(Throwable cause) {
            super(Code.CookieNotFoundException, cause);
        }
    }

    /**
     * Signal that cookie already exists when creating a new cookie.
     */
    public static class CookieExistException extends BookieException {
        public CookieExistException() {
            this("");
        }

        public CookieExistException(String reason) {
            super(Code.CookieExistsException, reason);
        }

        public CookieExistException(Throwable cause) {
            super(Code.CookieExistsException, cause);
        }
    }

    /**
     * Signal that error while accessing entry-log metadata map.
     */
    public static class EntryLogMetadataMapException extends BookieException {
        public EntryLogMetadataMapException(Throwable cause) {
            super(Code.EntryLogMetadataMapException, cause);
        }
    }

    /**
     * Signals that an exception occurs on upgrading a bookie.
     */
    public static class UpgradeException extends BookieException {
        public UpgradeException() {
            super(Code.UpgradeException);
        }

        public UpgradeException(Throwable cause) {
            super(Code.UpgradeException, cause);
        }

        public UpgradeException(String reason) {
            super(Code.UpgradeException, reason);
        }
    }

    /**
     * Signals when multiple ledger/journal directories are mounted in same disk partition.
     */
    public static class DiskPartitionDuplicationException extends BookieException {
        public DiskPartitionDuplicationException() {
            super(Code.DiskPartitionDuplicationException);
        }

        public DiskPartitionDuplicationException(Throwable cause) {
            super(Code.DiskPartitionDuplicationException, cause);
        }

        public DiskPartitionDuplicationException(String reason) {
            super(Code.DiskPartitionDuplicationException, reason);
        }
    }

    /**
     * Signal when bookie has problems on accessing metadata store.
     */
    public static class MetadataStoreException extends BookieException {

        public MetadataStoreException() {
            this("");
        }

        public MetadataStoreException(String reason) {
            super(Code.MetadataStoreException, reason);
        }

        public MetadataStoreException(Throwable cause) {
            super(Code.MetadataStoreException, cause);
        }

        public MetadataStoreException(String reason, Throwable cause) {
            super(Code.MetadataStoreException, reason, cause);
        }
    }

    /**
     * Signal when bookie has problems on accessing metadata store.
     */
    public static class UnknownBookieIdException extends BookieException {

        public UnknownBookieIdException() {
            super(Code.UnknownBookieIdException);
        }

        public UnknownBookieIdException(Throwable cause) {
            super(Code.UnknownBookieIdException, cause);
        }
    }

    /**
     * Signal when a ledger is in a limbo state and certain operations
     * cannot be performed on it.
     */
    public static class DataUnknownException extends BookieException {
        public DataUnknownException() {
            super(Code.DataUnknownException);
        }

        public DataUnknownException(Throwable t) {
            super(Code.DataUnknownException, t);
        }

        public DataUnknownException(String reason) {
            super(Code.DataUnknownException, reason);
        }

        public DataUnknownException(String reason, Throwable t) {
            super(Code.DataUnknownException, reason, t);
        }
    }
}
