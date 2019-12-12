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

package org.apache.bookkeeper.discover;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * Information about services exposed by a Bookie.
 */
public interface BookieServiceInfo {

    /**
     * Default empty implementation.
     */
    static BookieServiceInfo EMPTY = new BookieServiceInfo() {
        @Override
        public Iterator<String> keys() {
            return Collections.emptyIterator();
        }

        @Override
        public String get(String key, String defaultValue) {
            return defaultValue;
        }
    };
    
    /**
     * Default empty implementation.
     */
    static Supplier<BookieServiceInfo> NO_INFO = () -> EMPTY;


    /**
     * List all available entries.
     * Remove operation is not supported.
     * @return
     */
    Iterator<String> keys();

    /**
     * Return an entry, if the entry is not present the default value will be returned.
     * @param key the key
     * @param defaultValue the default value
     * @return the current mapping, if there is no mapping for key the defaultValue will be returned
     */
    String get(String key, String defaultValue);
}
