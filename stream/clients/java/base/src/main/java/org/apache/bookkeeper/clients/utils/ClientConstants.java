/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients.utils;

import org.apache.bookkeeper.common.util.Backoff.Jitter;
import org.apache.bookkeeper.common.util.Backoff.Jitter.Type;
import org.apache.bookkeeper.common.util.Backoff.Policy;

/**
 * Client related constants.
 */
public final class ClientConstants {

    public static final String TOKEN = "token";

    //
    // Backoff Related Constants
    //

    public static final int DEFAULT_BACKOFF_START_MS = 200;
    public static final int DEFAULT_BACKOFF_MAX_MS = 1000;
    public static final int DEFAULT_BACKOFF_MULTIPLIER = 2;
    public static final int DEFAULT_BACKOFF_RETRIES = 3;

    public static final Policy DEFAULT_BACKOFF_POLICY = Jitter.of(
        Type.EXPONENTIAL,
        DEFAULT_BACKOFF_START_MS,
        DEFAULT_BACKOFF_MAX_MS,
        DEFAULT_BACKOFF_RETRIES);

    public static final Policy DEFAULT_INFINIT_BACKOFF_POLICY = Jitter.of(
        Type.EXPONENTIAL,
        DEFAULT_BACKOFF_START_MS,
        DEFAULT_BACKOFF_MAX_MS);

}
