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
package org.apache.bookkeeper.metastore;

/**
 * A metastore watched event.
 */
public class MSWatchedEvent {
    /**
     * The metastore event type.
     */
    public enum EventType {CHANGED, REMOVED}

    String key;
    EventType type;

    public MSWatchedEvent(String key, EventType type) {
        this.key = key;
        this.type = type;
    }

    public EventType getType() {
        return type;
    }

    public String getKey(){
        return key;
    }
}
