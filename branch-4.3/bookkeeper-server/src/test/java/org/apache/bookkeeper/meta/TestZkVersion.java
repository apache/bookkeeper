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
package org.apache.bookkeeper.meta;

import org.junit.Test;
import org.junit.Assert;

import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Version.Occurred;

public class TestZkVersion {

    @Test(timeout=60000)
    public void testNullZkVersion() {
        ZkVersion zkVersion = new ZkVersion(99);
        try {
            zkVersion.compare(null);
            Assert.fail("Should fail comparing with null version.");
        } catch (NullPointerException npe) {
        }
    }

    @Test(timeout=60000)
    public void testInvalidVersion() {
        ZkVersion zkVersion = new ZkVersion(99);
        try {
            zkVersion.compare(new Version() {
                @Override
                public Occurred compare(Version v) {
                    return Occurred.AFTER;
                }
            });
            Assert.fail("Should not reach here!");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test(timeout=60000)
    public void testCompare() {
        ZkVersion zv = new ZkVersion(99);
        Assert.assertEquals(Occurred.AFTER, zv.compare(new ZkVersion(98)));
        Assert.assertEquals(Occurred.BEFORE, zv.compare(new ZkVersion(100)));
        Assert.assertEquals(Occurred.CONCURRENTLY, zv.compare(new ZkVersion(99)));
        Assert.assertEquals(Occurred.CONCURRENTLY, zv.compare(Version.ANY));
        Assert.assertEquals(Occurred.AFTER, zv.compare(Version.NEW));
    }
}
