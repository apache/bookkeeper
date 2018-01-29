/*
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
package org.apache.distributedlog.common.util;

/**
 * Permit manager for managing permits.
 */
public interface PermitManager {

    /**
     * A class present a permit managed by a permit manager.
     */
    interface Permit {
        Permit ALLOWED = () -> true;
        boolean isAllowed();
    }

    PermitManager UNLIMITED_PERMIT_MANAGER = new PermitManager() {
        @Override
        public Permit acquirePermit() {
            return Permit.ALLOWED;
        }

        @Override
        public void releasePermit(Permit permit) {
            // nop
        }

        @Override
        public boolean allowObtainPermits() {
            return true;
        }

        @Override
        public boolean disallowObtainPermits(Permit permit) {
            return false;
        }

        @Override
        public void close() {
            // nop
        }

    };

    /**
     * Obetain a permit from permit manager.
     *
     * @return permit.
     */
    Permit acquirePermit();

    /**
     * Release a given permit.
     *
     * @param permit
     *          permit to release
     */
    void releasePermit(Permit permit);

    /**
     * Allow obtaining permits.
     */
    boolean allowObtainPermits();

    /**
     * Disallow obtaining permits. Disallow needs to be performed under the context
     * of <i>permit</i>.
     *
     * @param permit
     *          permit context to disallow
     */
    boolean disallowObtainPermits(Permit permit);

    /**
     * Release the resources.
     */
    void close();
}
