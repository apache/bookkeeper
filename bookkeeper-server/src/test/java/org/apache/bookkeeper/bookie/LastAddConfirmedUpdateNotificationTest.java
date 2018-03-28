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

package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test of {@link LastAddConfirmedUpdateNotification}.
 */
public class LastAddConfirmedUpdateNotificationTest {

    @Test
    public void testGetters() {
        long lac = System.currentTimeMillis();
        LastAddConfirmedUpdateNotification notification = LastAddConfirmedUpdateNotification.of(lac);

        long timestamp = System.currentTimeMillis();
        assertEquals(lac, notification.getLastAddConfirmed());
        assertTrue(notification.getTimestamp() <= timestamp);

        notification.recycle();
    }

    @Test
    public void testRecycle() {
        long lac = System.currentTimeMillis();
        LastAddConfirmedUpdateNotification notification = LastAddConfirmedUpdateNotification.of(lac);
        notification.recycle();

        assertEquals(-1L, notification.getLastAddConfirmed());
        assertEquals(-1L, notification.getTimestamp());
    }

    @Test
    public void testFunc() {
        long lac = System.currentTimeMillis();
        LastAddConfirmedUpdateNotification notification = LastAddConfirmedUpdateNotification.FUNC.apply(lac);

        assertEquals(lac, notification.getLastAddConfirmed());
    }

}
