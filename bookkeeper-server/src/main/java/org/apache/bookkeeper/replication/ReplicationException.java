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

package org.apache.bookkeeper.replication;

import java.util.function.Function;

/**
 * Exceptions for use within the replication service.
 */
public abstract class ReplicationException extends Exception {

    public static final Function<Throwable, ReplicationException> EXCEPTION_HANDLER = cause -> {
        if (cause instanceof ReplicationException) {
            return (ReplicationException) cause;
        } else {
            return new UnavailableException(cause.getMessage(), cause);
        }
    };

    protected ReplicationException(String message, Throwable cause) {
        super(message, cause);
    }

    protected ReplicationException(String message) {
        super(message);
    }

    /**
     * The replication service has become unavailable.
     */
    public static class UnavailableException extends ReplicationException {
        private static final long serialVersionUID = 31872209L;

        public UnavailableException(String message, Throwable cause) {
            super(message, cause);
        }

        public UnavailableException(String message) {
            super(message);
        }
    }

    /**
     * Compatibility error. This version of the code, doesn't know how to
     * deal with the metadata it has found.
     */
    public static class CompatibilityException extends ReplicationException {
        private static final long serialVersionUID = 98551903L;

        public CompatibilityException(String message, Throwable cause) {
            super(message, cause);
        }

        public CompatibilityException(String message) {
            super(message);
        }
    }

    /**
     * Exception while auditing bookie-ledgers.
    */
    public static class BKAuditException extends ReplicationException {
        private static final long serialVersionUID = 95551905L;

        BKAuditException(String message, Throwable cause) {
            super(message, cause);
        }

        BKAuditException(String message) {
            super(message);
        }
    }
}
