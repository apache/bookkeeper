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
package org.apache.bookkeeper.common.testing;

import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Assertion utils.
 */
public final class MoreAsserts {

    private MoreAsserts() {}

    public static <T> void assertSetEquals(Set<T> expected, Set<T> actual) {
        SetView<T> diff = Sets.difference(expected, actual);
        assertTrue(
            "Expected set contains items not exist at actual set : " + diff.immutableCopy(),
            diff.isEmpty());
        diff = Sets.difference(actual, expected);
        assertTrue(
            "Actual set contains items not exist at expected set : " + diff.immutableCopy(),
            diff.isEmpty());
    }

    public static <T> void assertUtil(Predicate<T> predicate, Supplier<T> supplier) throws InterruptedException {
        while (!predicate.test(supplier.get())) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
    }

}
