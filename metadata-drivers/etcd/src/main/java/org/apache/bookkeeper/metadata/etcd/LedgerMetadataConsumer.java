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

package org.apache.bookkeeper.metadata.etcd;

import java.util.Objects;
import java.util.function.Consumer;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * A consumer wrapper over ledger metadata listener.
 */
class LedgerMetadataConsumer implements Consumer<Versioned<LedgerMetadata>> {

    private final long ledgerId;
    private final LedgerMetadataListener listener;
    private final Consumer<Long> onDeletedConsumer;

    LedgerMetadataConsumer(long ledgerId,
                           LedgerMetadataListener listener,
                           Consumer<Long> onDeletedConsumer) {
        this.ledgerId = ledgerId;
        this.listener = listener;
        this.onDeletedConsumer = onDeletedConsumer;
    }

    @Override
    public int hashCode() {
        return listener.hashCode();
    }

    @Override
    public String toString() {
        return listener.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LedgerMetadataConsumer)) {
            return false;
        }

        LedgerMetadataConsumer another = (LedgerMetadataConsumer) obj;
        return ledgerId == another.ledgerId
            && Objects.equals(listener, another.listener)
            && Objects.equals(onDeletedConsumer, another.onDeletedConsumer);
    }

    @Override
    public void accept(Versioned<LedgerMetadata> ledgerMetadataVersioned) {
        if (null == ledgerMetadataVersioned.getValue()) {
            onDeletedConsumer.accept(ledgerId);
        } else {
            listener.onChanged(ledgerId, ledgerMetadataVersioned);
        }
    }
}
