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
 * Exit code used to exit bookie server.
 */
public class ExitCode {
    // normal quit
    public static final int OK                  = 0;
    // invalid configuration
    public static final int INVALID_CONF        = 1;
    // exception running bookie server
    public static final int SERVER_EXCEPTION    = 2;
    // zookeeper is expired
    public static final int ZK_EXPIRED          = 3;
    // register bookie on zookeeper failed
    public static final int ZK_REG_FAIL         = 4;
    // exception running bookie
    public static final int BOOKIE_EXCEPTION    = 5;
}
