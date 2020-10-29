/*
 * Copyright 2020 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client.api;

/**
 * Utility container for listing ledgers.
 */
public interface ListLedgersResult extends AutoCloseable {

    /**
     * Creates a <code>LedgersIterator</code>.
     * This method must be called once per <code>ListLedgersResult</code> instance.
     * @return a <code>LedgersIterator</code> instance.
     */
    LedgersIterator iterator();

    /**
     * Creates a <code>Iterable</code>, which wraps a <code>LedgersIterator</code>.
     * This method must be called once per <code>ListLedgersResult</code> instance.
     * <br>
     * Metadata store access exceptions (<code>IOException</code>) are wrapped within a RuntimeException.
     * if you want to take care of these cases, it is better to use <code>LedgersIterator</code>.
     * @return a <code>Iterable</code> instance, containing ledger ids.
     */
    Iterable<Long> toIterable();

}
