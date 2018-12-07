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
package org.apache.bookkeeper.common.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test {@link ConfigKeyGroup}.
 */
public class ConfigKeyGroupTest {

    @Test
    public void testEquals() {
        ConfigKeyGroup grp1 = ConfigKeyGroup.builder("group1")
            .description("test group 1")
            .build();
        ConfigKeyGroup anotherGrp1 = ConfigKeyGroup.builder("group1")
            .description("test another group 1")
            .build();

        assertEquals(grp1, anotherGrp1);
    }

    @Test
    public void testOrdering() {
        ConfigKeyGroup grp10 = ConfigKeyGroup.builder("group1")
            .order(0)
            .build();
        ConfigKeyGroup grp20 = ConfigKeyGroup.builder("group2")
            .order(0)
            .build();
        ConfigKeyGroup grp21 = ConfigKeyGroup.builder("group2")
            .order(1)
            .build();

        assertTrue(ConfigKeyGroup.ORDERING.compare(grp10, grp20) < 0);
        assertTrue(ConfigKeyGroup.ORDERING.compare(grp20, grp21) < 0);
    }

}
