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
package org.apache.bookkeeper.bookie;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.function.Function;
import lombok.Getter;
import org.apache.bookkeeper.common.collections.RecyclableArrayList;
import org.apache.bookkeeper.common.util.Recyclable;
import org.apache.bookkeeper.common.util.Watcher;

/**
 * A signal object is used for notifying the observers when the {@code LastAddConfirmed} is advanced.
 *
 * <p>The signal object contains the latest {@code LastAddConfirmed} and when the {@code LastAddConfirmed} is advanced.
 */
@Getter
public class LastAddConfirmedUpdateNotification implements Recyclable {

    public static final Function<Long, LastAddConfirmedUpdateNotification> FUNC = lac -> of(lac);

    public static final RecyclableArrayList.Recycler<Watcher<LastAddConfirmedUpdateNotification>> WATCHER_RECYCLER =
        new RecyclableArrayList.Recycler<>();

    public static LastAddConfirmedUpdateNotification of(long lastAddConfirmed) {
        LastAddConfirmedUpdateNotification lac = RECYCLER.get();
        lac.lastAddConfirmed = lastAddConfirmed;
        lac.timestamp = System.currentTimeMillis();
        return lac;
    }

    private static final Recycler<LastAddConfirmedUpdateNotification> RECYCLER =
        new Recycler<LastAddConfirmedUpdateNotification>() {
            @Override
            protected LastAddConfirmedUpdateNotification newObject(Handle<LastAddConfirmedUpdateNotification> handle) {
                return new LastAddConfirmedUpdateNotification(handle);
            }
        };

    private final Handle<LastAddConfirmedUpdateNotification> handle;
    private long lastAddConfirmed;
    private long timestamp;

    public LastAddConfirmedUpdateNotification(Handle<LastAddConfirmedUpdateNotification> handle) {
        this.handle = handle;
    }

    @Override
    public void recycle() {
        this.lastAddConfirmed = -1L;
        this.timestamp = -1L;
        handle.recycle(this);
    }
}
