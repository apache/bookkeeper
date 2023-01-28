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

package org.apache.bookkeeper.common.util;

/**
 * Utility to make it easier to add context to exception messages.
 */
public class ExceptionMessageHelper {
    public StringBuilder sb = new StringBuilder();
    private boolean firstKV = true;

    public static ExceptionMessageHelper exMsg(String msg) {
        return new ExceptionMessageHelper(msg);
    }

    ExceptionMessageHelper(String msg) {
        sb.append(msg).append("(");
    }

    public ExceptionMessageHelper kv(String key, Object value) {
        if (firstKV) {
            firstKV = false;
        } else {
            sb.append(",");
        }
        sb.append(key).append("=").append(value.toString());
        return this;
    }

    public String toString() {
        return sb.append(")").toString();
    }
}
