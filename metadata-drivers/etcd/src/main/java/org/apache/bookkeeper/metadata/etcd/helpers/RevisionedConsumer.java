/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.function.Consumer;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Version.Occurred;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * A consumer that cache last version.
 */
public class RevisionedConsumer<T> implements Consumer<Versioned<T>> {

    protected final Consumer<Versioned<T>> consumer;
    protected volatile Version localVersion = null;

    protected RevisionedConsumer(Consumer<Versioned<T>> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void accept(Versioned<T> versionedVal) {
        synchronized (this) {
            if (localVersion != null
                && Occurred.BEFORE != localVersion.compare(versionedVal.getVersion())) {
                return;
            }
            localVersion = versionedVal.getVersion();
        }
        consumer.accept(versionedVal);
    }

    @Override
    public int hashCode() {
        return consumer.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Consumer)) {
            return false;
        }
        if (obj instanceof RevisionedConsumer) {
            return consumer.equals(((RevisionedConsumer) obj).consumer);
        } else {
            return consumer.equals(obj);
        }
    }

    @Override
    public String toString() {
        return consumer.toString();
    }
}
