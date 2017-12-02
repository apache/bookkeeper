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
package org.apache.bookkeeper.metastore;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Marker for metastore exceptions.
 */
@SuppressWarnings("serial")
public abstract class MSException extends Exception {

    /**
     * Return codes.
     */
    public enum Code {
        OK (0, "OK"),
        BadVersion (-1, "Version conflict"),
        NoKey (-2, "Key does not exist"),
        KeyExists (-3, "Key exists"),
        NoEntries (-4, "No entries found"),

        InterruptedException (-100, "Operation interrupted"),
        IllegalOp (-101, "Illegal operation"),
        ServiceDown (-102, "Metadata service is down"),
        OperationFailure(-103, "Operaion failed on metadata storage server side");

        private static final Map<Integer, Code> codes = new HashMap<Integer, Code>();

        static {
            for (Code c : EnumSet.allOf(Code.class)) {
                codes.put(c.code, c);
            }
        }

        private final int code;
        private final String description;

        private Code(int code, String description) {
            this.code = code;
            this.description = description;
        }

        /**
         * Get the int value for a particular Code.
         *
         * @return error code as integer
         */
        public int getCode() {
            return code;
        }

        /**
         * Get the description for a particular Code.
         *
         * @return error description
         */
        public String getDescription() {
            return description;
        }

        /**
         * Get the Code value for a particular integer error code.
         *
         * @param code int error code
         * @return Code value corresponding to specified int code, or null.
         */
        public static Code get(int code) {
            return codes.get(code);
        }
    }

    private final Code code;

    MSException(Code code, String errMsg) {
        super(code.getDescription() + " : " + errMsg);
        this.code = code;
    }

    MSException(Code code, String errMsg, Throwable cause) {
        super(code.getDescription() + " : " + errMsg, cause);
        this.code = code;
    }

    public Code getCode() {
        return this.code;
    }

    public static MSException create(Code code) {
        return create(code, "", null);
    }

    public static MSException create(Code code, String errMsg) {
        return create(code, errMsg, null);
    }

    public static MSException create(Code code, String errMsg, Throwable cause) {
        switch (code) {
            case BadVersion:
                return new BadVersionException(errMsg, cause);
            case NoKey:
                return new NoKeyException(errMsg, cause);
            case KeyExists:
                return new KeyExistsException(errMsg, cause);
            case InterruptedException:
                return new MSInterruptedException(errMsg, cause);
            case IllegalOp:
                return new IllegalOpException(errMsg, cause);
            case ServiceDown:
                return new ServiceDownException(errMsg, cause);
            case OperationFailure:
                return new OperationFailureException(errMsg, cause);
            case OK:
            default:
                throw new IllegalArgumentException("Invalid exception code");
        }
    }

    /**
     * A BadVersion exception.
     */
    public static class BadVersionException extends MSException {
        public BadVersionException(String errMsg) {
            super(Code.BadVersion, errMsg);
        }

        public BadVersionException(String errMsg, Throwable cause) {
            super(Code.BadVersion, errMsg, cause);
        }
    }

    /**
     * Exception in cases where there is no key.
     */
    public static class NoKeyException extends MSException {
        public NoKeyException(String errMsg) {
            super(Code.NoKey, errMsg);
        }

        public NoKeyException(String errMsg, Throwable cause) {
            super(Code.NoKey, errMsg, cause);
        }
    }

    /**
     * Exception would be thrown in a cursor if no entries found.
     */
    public static class NoEntriesException extends MSException {
        public NoEntriesException(String errMsg) {
            super(Code.NoEntries, errMsg);
        }

        public NoEntriesException(String errMsg, Throwable cause) {
            super(Code.NoEntries, errMsg, cause);
        }
    }

    /**
     * Key Exists Exception.
     */
    public static class KeyExistsException extends MSException {
        public KeyExistsException(String errMsg) {
            super(Code.KeyExists, errMsg);
        }

        public KeyExistsException(String errMsg, Throwable cause) {
            super(Code.KeyExists, errMsg, cause);
        }
    }

    /**
     * Metastore interruption exception.
     */
    public static class MSInterruptedException extends MSException {
        public MSInterruptedException(String errMsg) {
            super(Code.InterruptedException, errMsg);
        }

        public MSInterruptedException(String errMsg, Throwable cause) {
            super(Code.InterruptedException, errMsg, cause);
        }
    }

    /**
     * Illegal operation exception.
     */
    public static class IllegalOpException extends MSException {
        public IllegalOpException(String errMsg) {
            super(Code.IllegalOp, errMsg);
        }

        public IllegalOpException(String errMsg, Throwable cause) {
            super(Code.IllegalOp, errMsg, cause);
        }
    }

    /**
     * Service down exception.
     */
    public static class ServiceDownException extends MSException {
        public ServiceDownException(String errMsg) {
            super(Code.ServiceDown, errMsg);
        }

        public ServiceDownException(String errMsg, Throwable cause) {
            super(Code.ServiceDown, errMsg, cause);
        }
    }

    /**
     * Operation failure exception.
     */
    public static class OperationFailureException extends MSException {
        public OperationFailureException(String errMsg) {
            super(Code.OperationFailure, errMsg);
        }

        public OperationFailureException(String errMsg, Throwable cause) {
            super(Code.OperationFailure, errMsg, cause);
        }
    }
}
