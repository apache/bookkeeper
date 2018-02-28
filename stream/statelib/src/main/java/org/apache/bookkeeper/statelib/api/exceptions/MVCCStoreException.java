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

package org.apache.bookkeeper.statelib.api.exceptions;

import org.apache.bookkeeper.api.kv.result.Code;

/**
 * Exception thrown from a mvcc store.
 */
public class MVCCStoreException extends StateStoreRuntimeException {

    private static final long serialVersionUID = 1L;

    private final Code code;

    public MVCCStoreException(Code code, String msg) {
        super(msg + " : code = " + code);
        this.code = code;
    }

    public MVCCStoreException(Code code, Throwable t) {
        super(t);
        this.code = code;
    }

    public MVCCStoreException(Code code, String msg, Throwable t) {
        super(msg + " : code = " + code, t);
        this.code = code;
    }

    public Code getCode() {
        return code;
    }
}
