/*
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
package org.apache.bookkeeper.client.api;

import io.github.merlimat.slog.Logger;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;

/**
 * Builder-style interface to delete exiting ledgers.
 *
 * @since 4.6
 */
@Public
@Unstable
public interface DeleteBuilder extends OpBuilder<Void> {

    /**
     * Set the id of the ledger to be deleted.
     *
     * @param ledgerId
     *
     * @return the builder itself
     */
    DeleteBuilder withLedgerId(long ledgerId);

    /**
     * Inherit the context attributes of the given slog {@link Logger} on the logger used by the delete operation.
     * Every log statement emitted while executing the delete will carry the parent logger's context attributes,
     * in addition to the {@code ledgerId} attribute that is always added by the client.
     *
     * <p>Useful for correlating bookkeeper-client log output with the application's own request / tenant / trace
     * identifiers — typically the application has built a per-request logger via
     * {@code Logger.get(...).with().attr(...)...build()} and passes it here.
     *
     * @param parentLogger logger whose context attributes to inherit; {@code null} is treated as no extra context
     *
     * @return the builder itself
     */
    default DeleteBuilder withLoggerContext(Logger parentLogger) {
        return this;
    }

}
