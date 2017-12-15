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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.function.Function;
import org.apache.bookkeeper.common.collections.RecyclableArrayList;
import org.apache.bookkeeper.common.collections.RecyclableArrayList.Recycler;

/**
 * This class represents an watchable object, or "data"
 * in the model-view paradigm. It can be subclassed to represent an
 * object that the application wants to have watched.
 *
 * <p>An watchable object can have one or more watchers. An watcher
 * may be any object that implements interface <tt>Watcher</tt>. After an
 * watchable instance changes, an application calling the
 * <code>Watchable</code>'s <code>notifyWatchers</code> method
 * causes all of its watchers to be notified of the change by a call
 * to their <code>update</code> method.
 *
 * <p>A watcher is automatically removed from the watchers list once an event
 * is fired to the watcher.
 *
 * <p>Note that this notification mechanism has nothing to do with threads
 * and is completely separate from the <tt>wait</tt> and <tt>notify</tt>
 * mechanism of class <tt>Object</tt>.
 *
 * <p>When an watchable object is newly created, its set of watchers is
 * empty. If a same watcher is added multiple times to this watchable, it will
 * receive the notifications multiple times.
 */
public class Watchable<T> implements Recyclable {

    private final Recycler<Watcher<T>> recycler;
    private RecyclableArrayList<Watcher<T>> watchers;

    /** Construct an Watchable with zero watchers. */

    public Watchable(Recycler<Watcher<T>> recycler) {
        this.recycler = recycler;
        this.watchers = recycler.newInstance();
    }

    synchronized int getNumWatchers() {
        return this.watchers.size();
    }

    /**
     * Adds an watcher to the set of watchers for this object, provided
     * that it is not the same as some watcher already in the set.
     * The order in which notifications will be delivered to multiple
     * watchers is not specified. See the class comment.
     *
     * @param  w an watcher to be added.
     * @return true if a watcher is added to the list successfully, otherwise false.
     * @throws NullPointerException   if the parameter o is null.
     */
    public synchronized boolean addWatcher(Watcher<T> w) {
        checkNotNull(w, "Null watcher is provided");
        return watchers.add(w);
    }

    /**
     * Deletes an watcher from the set of watcher of this object.
     * Passing <CODE>null</CODE> to this method will have no effect.
     * @param w the watcher to be deleted.
     */
    public synchronized boolean deleteWatcher(Watcher<T> w) {
        return watchers.remove(w);
    }

    /**
     * Notify the watchers with the update <i>value</i>.
     *
     * @param value value to notify
     */
    public <R> void notifyWatchers(Function<R, T> valueFn, R value) {
        RecyclableArrayList<Watcher<T>> watchersLocal;
        synchronized (this) {
            watchersLocal = watchers;
            watchers = recycler.newInstance();
        }

        for (Watcher<T> watcher : watchersLocal) {
            watcher.update(valueFn.apply(value));
        }
        watchersLocal.recycle();
    }

    /**
     * Clears the watcher list so that this object no longer has any watchers.
     */
    public synchronized void deleteWatchers() {
        watchers.clear();
    }

    @Override
    public synchronized void recycle() {
        watchers.recycle();
    }
}
