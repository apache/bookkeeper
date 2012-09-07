package org.apache.bookkeeper.bookie;

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

@SuppressWarnings("serial")
public abstract class BookieException extends Exception {

    private int code;
    public BookieException(int code) {
        this.code = code;
    }

    public BookieException(int code, Throwable t) {
        super(t);
    }

    public BookieException(int code, String reason) {
        super(reason);
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
        default:
            return new BookieIllegalOpException();
        }
    }

    public interface Code {
        int OK = 0;
        int UnauthorizedAccessException = -1;

        int IllegalOpException = -100;
        int LedgerFencedException = -101;

        int InvalidCookieException = -102;
        int UpgradeException = -103;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }

    public String getMessage(int code) {
        String err = "Invalid operation";
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

    public static class BookieUnauthorizedAccessException extends BookieException {
        public BookieUnauthorizedAccessException() {
            super(Code.UnauthorizedAccessException);
        }
    }

    public static class BookieIllegalOpException extends BookieException {
        public BookieIllegalOpException() {
            super(Code.UnauthorizedAccessException);
        }
    }

    public static class LedgerFencedException extends BookieException {
        public LedgerFencedException() {
            super(Code.LedgerFencedException);
        }
    }

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
}
