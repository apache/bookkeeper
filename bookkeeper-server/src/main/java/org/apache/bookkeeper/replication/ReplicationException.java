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

/**
 * Exceptions for use within the replication service
 */
abstract class ReplicationException extends Exception {
    protected ReplicationException(String message, Throwable cause) {
        super(message, cause);
    }

    protected ReplicationException(String message) {
        super(message);
    }

    /**
     * The replication service has become unavailable
     */
    static class UnavailableException extends ReplicationException {
        private static final long serialVersionUID = 31872209L;

        UnavailableException(String message, Throwable cause) {
            super(message, cause);
        }

        UnavailableException(String message) {
            super(message);
        }
    }

    /**
     * Compatibility error. This version of the code, doesn't know how to
     * deal with the metadata it has found.
     */
    static class CompatibilityException extends ReplicationException {
        private static final long serialVersionUID = 98551903L;

        CompatibilityException(String message, Throwable cause) {
            super(message, cause);
        }

        CompatibilityException(String message) {
            super(message);
        }
    }
}
