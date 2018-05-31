/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Unit test for {@link StorageClientSettings}.
 */
public class TestStorageClientSettings {

    @Test
    public void testDefault() {
        StorageClientSettings settings = StorageClientSettings.newBuilder()
            .serviceUri("bk://127.0.0.1:4181/")
            .build();
        assertEquals("bk://127.0.0.1:4181/", settings.serviceUri());
        assertEquals(Runtime.getRuntime().availableProcessors(), settings.numWorkerThreads());
        assertTrue(settings.usePlaintext());
        assertFalse(settings.clientName().isPresent());
    }

    @Test
    public void testEmptyBuilder() {
        try {
            StorageClientSettings.newBuilder().build();
            fail("Should fail with missing endpoints");
        } catch (IllegalStateException iae) {
            assertEquals("Not set: [serviceUri]", iae.getMessage());
        }
    }

}
