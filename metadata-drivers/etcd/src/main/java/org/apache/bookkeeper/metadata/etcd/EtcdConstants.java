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
package org.apache.bookkeeper.metadata.etcd;

import com.coreos.jetcd.data.ByteSequence;

/**
 * Constants used in the Etcd metadata drivers.
 */
final class EtcdConstants {

    private EtcdConstants() {}

    public static final String END_SEP = "0";

    public static final String LAYOUT_NODE = "layout";
    public static final String INSTANCEID_NODE = "instanceid";
    public static final String COOKIES_NODE = "cookies";
    public static final String LEDGERS_NODE = "ledgers";
    public static final String BUCKETS_NODE = "buckets";

    //
    // membership related constants
    //

    public static final String MEMBERS_NODE = "bookies";
    public static final String WRITEABLE_NODE = "writable";
    public static final String READONLY_NODE = "readonly";

    //
    // underreplication related constants
    //

    public static final String UR_NODE = "underreplication";

    public static final ByteSequence EMPTY_BS  = ByteSequence.fromBytes(new byte[0]);

}
