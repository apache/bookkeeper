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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test {@link ConfigKey}.
 */
public class ConfigKeyTest {

    /**
     * Test Function A.
     */
    private static class TestFunctionA implements Function<String, String> {

        @Override
        public String apply(String s) {
            return s + "!";
        }
    }

    /**
     * Test Function B.
     */
    private static class TestFunctionB implements Function<String, String> {

        @Override
        public String apply(String s) {
            return s + "!";
        }
    }

    /**
     * Test Function C.
     */
    private static class TestFunctionC implements Function<String, String> {

        @Override
        public String apply(String s) {
            return s + "!";
        }
    }

    @Rule
    public final TestName runtime = new TestName();

    @Test
    public void testValidateRequiredField() {
        String keyName = runtime.getMethodName();
        Configuration conf = new ConcurrentConfiguration();
        ConfigKey key = ConfigKey.builder(keyName)
            .required(true)
            .build();

        try {
            key.validate(conf);
            fail("Required key should exist in the configuration");
        } catch (ConfigException ce) {
            // expected
        }
    }

    @Test
    public void testValidateFieldSuccess() throws ConfigException {
        String keyName = runtime.getMethodName();
        Validator validator = mock(Validator.class);
        when(validator.validate(anyString(), any())).thenReturn(true);
        Configuration conf = new ConcurrentConfiguration();
        conf.setProperty(keyName, "test-value");
        ConfigKey key = ConfigKey.builder(keyName)
            .validator(validator)
            .build();

        key.validate(conf);
        verify(validator, times(1)).validate(eq(keyName), eq("test-value"));
    }

    @Test
    public void testValidateFieldFailure() {
        String keyName = runtime.getMethodName();
        Validator validator = mock(Validator.class);
        when(validator.validate(anyString(), any())).thenReturn(false);
        Configuration conf = new ConcurrentConfiguration();
        conf.setProperty(keyName, "test-value");
        ConfigKey key = ConfigKey.builder(keyName)
            .validator(validator)
            .build();

        try {
            key.validate(conf);
            fail("Should fail validation if validator#validate returns false");
        } catch (ConfigException ce) {
            // expected
        }
        verify(validator, times(1)).validate(eq(keyName), eq("test-value"));
    }

    @Test
    public void testGetLong() {
        String keyName = runtime.getMethodName();
        long defaultValue = System.currentTimeMillis();
        ConfigKey key = ConfigKey.builder(keyName)
            .required(true)
            .type(Type.LONG)
            .defaultValue(defaultValue)
            .build();

        Configuration conf = new ConcurrentConfiguration();

        // get default value
        assertEquals(defaultValue, key.getLong(conf));
        assertEquals(defaultValue, key.get(conf));

        // set value
        long newValue = System.currentTimeMillis() * 2;
        key.set(conf, newValue);
        assertEquals(newValue, key.getLong(conf));
        assertEquals(newValue, key.get(conf));
    }

    @Test
    public void testGetInt() {
        String keyName = runtime.getMethodName();
        int defaultValue = ThreadLocalRandom.current().nextInt(10000);
        ConfigKey key = ConfigKey.builder(keyName)
            .required(true)
            .type(Type.INT)
            .defaultValue(defaultValue)
            .build();

        Configuration conf = new ConcurrentConfiguration();

        // get default value
        assertEquals(defaultValue, key.getInt(conf));
        assertEquals(defaultValue, key.get(conf));

        // set value
        int newValue = defaultValue * 2;
        key.set(conf, newValue);
        assertEquals(newValue, key.getInt(conf));
        assertEquals(newValue, key.get(conf));
    }

    @Test
    public void testGetShort() {
        String keyName = runtime.getMethodName();
        short defaultValue = (short) ThreadLocalRandom.current().nextInt(10000);
        ConfigKey key = ConfigKey.builder(keyName)
            .required(true)
            .type(Type.SHORT)
            .defaultValue(defaultValue)
            .build();

        Configuration conf = new ConcurrentConfiguration();

        // get default value
        assertEquals(defaultValue, key.getShort(conf));
        assertEquals(defaultValue, key.get(conf));

        // set value
        short newValue = (short) (defaultValue * 2);
        key.set(conf, newValue);
        assertEquals(newValue, key.getShort(conf));
        assertEquals(newValue, key.get(conf));
    }

    @Test
    public void testGetDouble() {
        String keyName = runtime.getMethodName();
        double defaultValue = ThreadLocalRandom.current().nextDouble(10000.0f);
        ConfigKey key = ConfigKey.builder(keyName)
            .required(true)
            .type(Type.DOUBLE)
            .defaultValue(defaultValue)
            .build();

        Configuration conf = new ConcurrentConfiguration();

        // get default value
        assertEquals(defaultValue, key.getDouble(conf), 0.0001);
        assertEquals(defaultValue, key.get(conf));

        // set value
        double newValue = (defaultValue * 2);
        key.set(conf, newValue);
        assertEquals(newValue, key.getDouble(conf), 0.0001);
        assertEquals(newValue, key.get(conf));
    }

    @Test
    public void testGetBoolean() {
        String keyName = runtime.getMethodName();
        boolean defaultValue = ThreadLocalRandom.current().nextBoolean();
        ConfigKey key = ConfigKey.builder(keyName)
            .required(true)
            .type(Type.BOOLEAN)
            .defaultValue(defaultValue)
            .build();

        Configuration conf = new ConcurrentConfiguration();

        // get default value
        assertEquals(defaultValue, key.getBoolean(conf));
        assertEquals(defaultValue, key.get(conf));

        // set value
        boolean newValue = !defaultValue;
        key.set(conf, newValue);
        assertEquals(newValue, key.getBoolean(conf));
        assertEquals(newValue, key.get(conf));
    }

    @Test
    public void testGetList() {
        String keyName = runtime.getMethodName();
        List<String> defaultList = Lists.newArrayList(
            "item1", "item2", "item3"
        );
        ConfigKey key = ConfigKey.builder(keyName)
            .required(true)
            .type(Type.LIST)
            .defaultValue(defaultList)
            .build();

        Configuration conf = new CompositeConfiguration();

        // get default value
        assertEquals(defaultList, key.getList(conf));
        assertEquals(defaultList, key.get(conf));

        // set value
        List<String> newList = Lists.newArrayList(
            "item4", "item5", "item6"
        );
        key.set(conf, newList);
        assertEquals(newList, key.getList(conf));
        assertEquals(newList, key.get(conf));

        // set string value
        newList = Lists.newArrayList(
            "item7", "item8", "item9"
        );
        conf.setProperty(key.name(), Lists.newArrayList("item7", "item8", "item9"));
        assertEquals(newList, key.getList(conf));
        assertEquals(newList, key.get(conf));
    }

    @Test
    public void testGetClass() {
        String keyName = runtime.getMethodName();
        Class defaultClass = TestFunctionA.class;
        ConfigKey key = ConfigKey.builder(keyName)
            .required(true)
            .type(Type.CLASS)
            .defaultValue(defaultClass)
            .build();

        Configuration conf = new CompositeConfiguration();

        // get default value
        assertEquals(defaultClass, key.getClass(conf));
        assertEquals(defaultClass, key.get(conf));

        // set value
        Class newClass = TestFunctionB.class;
        key.set(conf, newClass);
        assertEquals(newClass, key.getClass(conf));
        assertEquals(newClass, key.get(conf));

        // set string value
        String newClassName = TestFunctionC.class.getName();
        conf.setProperty(key.name(), newClassName);
        assertEquals(TestFunctionC.class, key.getClass(conf));
        assertEquals(TestFunctionC.class, key.get(conf));
    }

    @Test
    public void testGetString() {
        String keyName = runtime.getMethodName();
        String defaultValue = "default-string-value";
        ConfigKey key = ConfigKey.builder(keyName)
            .required(true)
            .type(Type.STRING)
            .defaultValue(defaultValue)
            .build();

        Configuration conf = new CompositeConfiguration();

        // get default value
        assertEquals(defaultValue, key.getString(conf));
        assertEquals(defaultValue, key.get(conf));

        // set value
        String newValue = "new-string-value";
        key.set(conf, newValue);
        assertEquals(newValue, key.getString(conf));
        assertEquals(newValue, key.get(conf));
    }

}
