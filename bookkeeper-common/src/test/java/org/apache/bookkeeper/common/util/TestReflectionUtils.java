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

package org.apache.bookkeeper.common.util;

import static org.apache.bookkeeper.common.util.ReflectionUtils.forName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Unit test of {@link ReflectionUtils}.
 */
public class TestReflectionUtils {

    interface InterfaceA {
    }

    interface InterfaceB {
    }

    private static class ClassA implements InterfaceA {
    }

    private static class ClassB implements InterfaceB {
    }

    @Test
    public void testForNameClassNotFound() {
        try {
            forName(
                "test.for.name.class.not.found",
                Object.class);
            fail("Should fail if class not found");
        } catch (RuntimeException re) {
            assertTrue(re.getCause() instanceof ClassNotFoundException);
        }
    }

    @Test
    public void testForNameUnassignable() {
        try {
            forName(
                ClassA.class.getName(),
                InterfaceB.class);
            fail("Should fail if class is not assignable");
        } catch (RuntimeException re) {
            assertEquals(
                ClassA.class.getName() + " not " + InterfaceB.class.getName(),
                re.getMessage());
        }
    }

    @Test
    public void testForName() throws Exception {
        Class<? extends InterfaceB> theCls = forName(
            ClassB.class.getName(),
            InterfaceB.class);
        assertEquals(ClassB.class, theCls);
    }

}
