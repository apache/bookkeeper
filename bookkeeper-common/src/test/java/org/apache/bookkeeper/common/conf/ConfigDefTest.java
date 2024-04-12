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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.conf.validators.ClassValidator;
import org.apache.bookkeeper.common.conf.validators.RangeValidator;
import org.junit.Test;

/**
 * Unit test {@link ConfigDef}.
 */
@Slf4j
public class ConfigDefTest {

    private static class TestConfig {

        private static final ConfigKeyGroup group1 = ConfigKeyGroup.builder("group1")
            .description("Group 1 Settings")
            .order(1)
            .build();

        private static final ConfigKey key11 = ConfigKey.builder("key11")
            .type(Type.LONG)
            .group(group1)
            .validator(RangeValidator.atLeast(1000))
            .build();

        private static final ConfigKeyGroup group2 = ConfigKeyGroup.builder("group2")
            .description("Group 2 Settings")
            .order(2)
            .build();

        private static final ConfigKey key21 = ConfigKey.builder("key21")
            .type(Type.LONG)
            .group(group2)
            .validator(RangeValidator.atMost(1000))
            .orderInGroup(2)
            .build();

        private static final ConfigKey key22 = ConfigKey.builder("key22")
            .type(Type.STRING)
            .group(group2)
            .validator(ClassValidator.of(Runnable.class))
            .orderInGroup(1)
            .build();

    }

    private static class TestConfig2 {

        private static final ConfigKeyGroup emptyGroup = ConfigKeyGroup.builder("empty_group")
            .description("Empty Group Settings")
            .order(1)
            .build();

        private static final ConfigKeyGroup group1 = ConfigKeyGroup.builder("group1")
            .description("This is a very long description : Lorem ipsum dolor sit amet,"
                + " consectetur adipiscing elit. Maecenas bibendum ac felis id commodo."
                + " Etiam mauris purus, fringilla id tempus in, mollis vel orci. Duis"
                + " ultricies at erat eget iaculis.")
            .order(2)
            .build();

        private static final ConfigKey intKey = ConfigKey.builder("int_key")
            .type(Type.INT)
            .description("it is an int key")
            .group(group1)
            .validator(RangeValidator.atLeast(1000))
            .build();

        private static final ConfigKey longKey = ConfigKey.builder("long_key")
            .type(Type.LONG)
            .description("it is a long key")
            .group(group1)
            .validator(RangeValidator.atMost(1000))
            .build();

        private static final ConfigKey shortKey = ConfigKey.builder("short_key")
            .type(Type.SHORT)
            .description("it is a short key")
            .group(group1)
            .validator(RangeValidator.between(500, 1000))
            .build();

        private static final ConfigKey doubleKey = ConfigKey.builder("double_key")
            .type(Type.DOUBLE)
            .description("it is a double key")
            .group(group1)
            .validator(RangeValidator.between(1234.0f, 5678.0f))
            .build();

        private static final ConfigKey boolKey = ConfigKey.builder("bool_key")
            .type(Type.BOOLEAN)
            .description("it is a bool key")
            .group(group1)
            .build();

        private static final ConfigKey classKey = ConfigKey.builder("class_key")
            .type(Type.CLASS)
            .description("it is a class key")
            .validator(ClassValidator.of(Runnable.class))
            .group(group1)
            .build();

        private static final ConfigKey listKey = ConfigKey.builder("list_key")
            .type(Type.LIST)
            .description("it is a list key")
            .group(group1)
            .build();

        private static final ConfigKey stringKey = ConfigKey.builder("string_key")
            .type(Type.STRING)
            .description("it is a string key")
            .group(group1)
            .build();

        private static final ConfigKeyGroup group2 = ConfigKeyGroup.builder("group2")
            .description("This group has short description")
            .order(3)
            .build();

        private static final ConfigKey keyWithSince = ConfigKey.builder("key_with_since")
            .type(Type.STRING)
            .description("it is a string key with since")
            .since("4.7.0")
            .group(group2)
            .orderInGroup(10)
            .build();

        private static final ConfigKey keyWithDocumentation = ConfigKey.builder("key_with_short_documentation")
            .type(Type.STRING)
            .description("it is a string key with documentation")
            .documentation("it has a short documentation")
            .group(group2)
            .orderInGroup(9)
            .build();

        private static final ConfigKey keyWithLongDocumentation =
            ConfigKey.builder("key_long_short_documentation")
                .type(Type.STRING)
                .description("it is a string key with documentation")
                .documentation("it has a long documentation : Lorem ipsum dolor sit amet,"
                    + " consectetur adipiscing elit. Maecenas bibendum ac felis id commodo."
                    + " Etiam mauris purus, fringilla id tempus in, mollis vel orci. Duis"
                    + " ultricies at erat eget iaculis.")
                .group(group2)
                .orderInGroup(8)
                .build();

        private static final ConfigKey keyWithDefaultValue = ConfigKey.builder("key_with_default_value")
            .type(Type.STRING)
            .description("it is a string key with default value")
            .defaultValue("this-is-a-test-value")
            .group(group2)
            .orderInGroup(7)
            .build();

        private static final ConfigKey keyWithOptionalValues = ConfigKey.builder("key_with_optional_values")
            .type(Type.STRING)
            .description("it is a string key with optional values")
            .defaultValue("this-is-a-default-value")
            .optionValues(Lists.newArrayList(
                "item1", "item2", "item3", "item3"
            ))
            .group(group2)
            .orderInGroup(6)
            .build();

        private static final ConfigKey deprecatedKey = ConfigKey.builder("deprecated_key")
            .type(Type.STRING)
            .deprecated(true)
            .description("it is a deprecated key")
            .group(group2)
            .orderInGroup(5)
            .build();

        private static final ConfigKey deprecatedKeyWithSince = ConfigKey.builder("deprecated_key_with_since")
            .type(Type.STRING)
            .deprecated(true)
            .deprecatedSince("4.3.0")
            .description("it is a deprecated key with since")
            .group(group2)
            .orderInGroup(4)
            .build();

        private static final ConfigKey deprecatedKeyWithReplacedKey =
            ConfigKey.builder("deprecated_key_with_replaced_key")
                .type(Type.STRING)
                .deprecated(true)
                .deprecatedByConfigKey("key_with_optional_values")
                .description("it is a deprecated key with replaced key")
                .group(group2)
                .orderInGroup(3)
                .build();

        private static final ConfigKey deprecatedKeyWithSinceAndReplacedKey =
            ConfigKey.builder("deprecated_key_with_since_and_replaced_key")
                .type(Type.STRING)
                .deprecated(true)
                .deprecatedSince("4.3.0")
                .deprecatedByConfigKey("key_with_optional_values")
                .description("it is a deprecated key with since and replaced key")
                .group(group2)
                .orderInGroup(2)
                .build();

        private static final ConfigKey requiredKey = ConfigKey.builder("required_key")
            .type(Type.STRING)
            .required(true)
            .description("it is a required key")
            .group(group2)
            .orderInGroup(1)
            .build();

    }

    @Test
    public void testBuildConfigDef() {
        ConfigDef configDef = ConfigDef.of(TestConfig.class);
        assertEquals(2, configDef.getGroups().size());

        Iterator<ConfigKeyGroup> grpIter = configDef.getGroups().iterator();

        // iterate over group 1
        assertTrue(grpIter.hasNext());
        ConfigKeyGroup group1 = grpIter.next();
        assertSame(TestConfig.group1, group1);
        Set<ConfigKey> keys = configDef.getSettings().get(group1.name());
        assertNotNull(keys);
        assertEquals(1, keys.size());
        assertEquals(TestConfig.key11, keys.iterator().next());

        // iterate over group 2
        assertTrue(grpIter.hasNext());
        ConfigKeyGroup group2 = grpIter.next();
        assertSame(TestConfig.group2, group2);
        keys = configDef.getSettings().get(group2.name());
        assertNotNull(keys);
        assertEquals(2, keys.size());
        Iterator<ConfigKey> keyIter = keys.iterator();
        assertEquals(TestConfig.key22, keyIter.next());
        assertEquals(TestConfig.key21, keyIter.next());
        assertFalse(keyIter.hasNext());

        // no more group
        assertFalse(grpIter.hasNext());
    }

    @Test
    public void testSaveConfigDef() throws IOException  {
        StringBuilder sb = new StringBuilder();
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream("test_conf_2.conf");
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append(System.lineSeparator());
            }
        }
        String confData = sb.toString();

        ConfigDef configDef = ConfigDef.of(TestConfig2.class);
        String readConf;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            configDef.save(baos);
            readConf = baos.toString();
            log.info("\n{}", readConf);
        }

        assertEquals(confData, readConf);
    }

}
