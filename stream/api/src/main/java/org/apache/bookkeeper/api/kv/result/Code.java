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

package org.apache.bookkeeper.api.kv.result;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Status Code.
 */
@RequiredArgsConstructor
@Getter
public enum Code {

    OK(0),
    INTERNAL_ERROR(-1),
    INVALID_ARGUMENT(-2),
    ILLEGAL_OP(-3),
    UNEXPECTED(-4),
    BAD_REVISION(-5),
    SMALLER_REVISION(-6),
    KEY_NOT_FOUND(-7),
    KEY_EXISTS(-8);

    private final int code;

}
