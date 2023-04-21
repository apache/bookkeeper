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

package org.apache.bookkeeper.metadata.etcd.helpers;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * Iterator over a range of key/value pairs.
 */
public class KeyIterator<T> {

    private final KeyStream<T> stream;
    private CompletableFuture<List<T>> readFuture = null;
    private boolean hasNext = true;
    private List<T> keys = null;

    public KeyIterator(KeyStream<T> stream) {
        this.stream = stream;
    }

    public synchronized boolean hasNext() throws Exception {
        if (hasNext) {
            if (null == readFuture) {
                readFuture = stream.readNext();
            }
            keys = result(readFuture);
            if (keys.isEmpty()) {
                hasNext = false;
            }
            return hasNext;
        } else {
            return false;
        }
    }

    public synchronized List<T> next() throws Exception {
        try {
            if (!hasNext()) {
                throw new NoSuchElementException("Reach end of key stream");
            }
            return keys;
        } finally {
            // read next
            readFuture = stream.readNext();
        }
    }

}
