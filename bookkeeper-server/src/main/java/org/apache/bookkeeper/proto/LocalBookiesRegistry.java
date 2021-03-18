/**
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
package org.apache.bookkeeper.proto;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.net.BookieId;

/**
 * Local registry for embedded Bookies.
 */
public class LocalBookiesRegistry {

    private static final ConcurrentHashMap<BookieId, Boolean> localBookiesRegistry =
        new ConcurrentHashMap<>();

    static void registerLocalBookieAddress(BookieId address) {
        localBookiesRegistry.put(address, Boolean.TRUE);
    }
    static void unregisterLocalBookieAddress(BookieId address) {
        if (address != null) {
            localBookiesRegistry.remove(address);
        }
    }
    public static boolean isLocalBookie(BookieId address) {
        return localBookiesRegistry.containsKey(address);
    }

}
