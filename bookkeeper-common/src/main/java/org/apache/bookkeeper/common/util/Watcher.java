/*
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
 */

package org.apache.bookkeeper.common.util;

/**
 * A class can implement the <code>Watcher</code> interface when it
 * wants to be informed of <i>one-time</i> changes in watchable objects.
 */
public interface Watcher<T> {

    /**
     * This method is called whenever the watched object is changed. An
     * application calls an <tt>Watchable</tt> object's
     * <code>notifyWatchers</code> method to have all the object's
     * watchers notified of the change.
     *
     * @param value the updated value of a watchable
     */
    void update(T value);

}
