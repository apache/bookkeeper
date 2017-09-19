/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *pa
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.client.api;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BKException;

/**
 * Builder-style interface to open exiting ledgers
 */
public interface OpenBuilder extends OpBuilder<ReadHandle> {

    OpenBuilder withLedgerId(long ledgerId);

    OpenBuilder withRecovery(boolean recovery);

    OpenBuilder withPassword(byte[] password);

    OpenBuilder withDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType digestType);

    CompletableFuture<ReadHandle> execute();

    ReadHandle open() throws BKException, InterruptedException;

}
