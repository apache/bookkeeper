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
package org.apache.bookkeeper.meta;

import java.io.IOException;
import java.util.function.Consumer;
import org.apache.bookkeeper.net.BookieId;

/**
 * Interface to handle the ledger auditor election.
 */
public interface LedgerAuditorManager extends AutoCloseable {

    /**
     * Events that can be triggered by the LedgerAuditorManager.
     */
    enum AuditorEvent {
        SessionLost,
        VoteWasDeleted,
    }

    /**
     * Try to become the auditor. If there's already another auditor, it will wait until this
     * current instance has become the auditor.
     *
     * <p>This method may run concurrently with {@link #close()}. A blocked election wait must
     * respond to either closing this manager or interrupting the election thread, so the caller
     * can cancel an election during shutdown. Implementations must not swallow interruption and
     * continue waiting indefinitely. After the manager is closed this method must not admit new
     * election participation, and an election cancelled by close may return without becoming auditor.
     *
     * @param bookieId the identifier for current bookie
     * @param listener listener that will receive AuditorEvent notifications
     */
    void tryToBecomeAuditor(String bookieId, Consumer<AuditorEvent> listener) throws IOException, InterruptedException;

    /**
     * Return the information regarding the current auditor.
     * @return
     */
    BookieId getCurrentAuditor() throws IOException, InterruptedException;

    /**
     * Close this manager and release its election resources.
     *
     * <p>This method may be called while {@link #tryToBecomeAuditor(String, Consumer)} is waiting
     * for leadership. Implementations should release such waits directly; otherwise they must
     * support interruption of the election thread as described above. Closing may wait for an
     * admitted metadata operation, such as vote creation, to finish so its resources can be released.
     * It must not wait for the election invocation itself to return: the caller may interrupt the
     * election thread after close returns. The caller must then wait for the election invocation
     * to exit before releasing resources shared with that invocation.
     *
     * @throws Exception if closing election resources fails
     */
    @Override
    void close() throws Exception;
}
