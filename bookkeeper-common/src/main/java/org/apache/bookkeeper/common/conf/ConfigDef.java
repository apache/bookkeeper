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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

/**
 * A definition of a configuration instance.
 */
@Slf4j
@Getter
public class ConfigDef {

    /**
     * Builder to build a configuration definition.
     */
    public static class Builder {

        private final Set<ConfigKeyGroup> groups = new TreeSet<>(ConfigKeyGroup.ORDERING);
        private final Map<String, Set<ConfigKey>> settings = new HashMap<>();

        private Builder() {}

        /**
         * Add the config key group to the builder.
         *
         * @param group config key group
         * @return builder to build this configuration def
         */
        public Builder withConfigKeyGroup(ConfigKeyGroup group) {
            groups.add(group);
            return this;
        }

        /**
         * Add the config key to the builder.
         *
         * @param key the key to add to the builder.
         * @return builder to build this configuration def
         */
        public Builder withConfigKey(ConfigKey key) {
            ConfigKeyGroup group = key.group();
            Set<ConfigKey> keys;
            String groupName;
            if (null == group) {
                groupName = "";
            } else {
                groupName = group.name();
                groups.add(group);
            }
            keys = settings.computeIfAbsent(groupName, name -> new TreeSet<>(ConfigKey.ORDERING));
            keys.add(key);
            return this;
        }

        public ConfigDef build() {
            checkArgument(
                Sets.difference(
                    groups.stream().map(group -> group.name()).collect(Collectors.toSet()),
                    settings.keySet()
                ).isEmpty(),
                "Configuration Key Groups doesn't match with keys");
            return new ConfigDef(groups, settings);
        }

    }

    /**
     * Create a builder to build a config def.
     *
     * @return builder to build a config def.
     */
    public static Builder builder() {
        return new Builder();
    }

    private final Set<ConfigKeyGroup> groups;
    private final Map<String, Set<ConfigKey>> settings;
    private final Map<String, ConfigKey> keys;

    private ConfigDef(Set<ConfigKeyGroup> groups,
                      Map<String, Set<ConfigKey>> settings) {
        this.groups = groups;
        this.settings = settings;
        this.keys = settings.values()
            .stream()
            .flatMap(keys -> keys.stream())
            .collect(Collectors.toSet())
            .stream()
            .collect(Collectors.toMap(
                key -> key.name(),
                key -> key
            ));
    }

    /**
     * Validate if the provided <tt>conf</tt> is a valid configuration of this configuration definition.
     *
     * @param conf the configuration to validate
     */
    public void validate(Configuration conf) throws ConfigException {
        for (ConfigKey key : keys.values()) {
            key.validate(conf);
        }
    }

    /**
     * Build the config definitation of a config class.
     *
     * @param configClass config class
     * @return config definition.
     */
    @SuppressWarnings("unchecked")
    public static ConfigDef of(Class configClass) {
        ConfigDef.Builder builder = ConfigDef.builder();

        Field[] fields = configClass.getDeclaredFields();
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType().equals(ConfigKey.class)) {
                field.setAccessible(true);
                try {
                    builder.withConfigKey((ConfigKey) field.get(null));
                } catch (IllegalAccessException e) {
                    log.error("Illegal to access {}#{}", configClass.getSimpleName(), field.getName(), e);
                }
            }
        }

        return builder.build();
    }

    //
    // Methods to save the configuration to an {@link OutputStream}
    //

    private static final int MAX_COLUMN_SIZE = 80;
    private static final String COMMENT_PREFIX = "# ";

    public void save(Path path) throws IOException  {
        try (OutputStream stream = Files.newOutputStream(
            path, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            save(stream);
        }
    }

    public void save(OutputStream os) throws IOException {
        try (PrintStream ps = new PrintStream(os, false, UTF_8.name())) {
            save(ps);
            ps.flush();
        }
    }

    private void writeNSharps(PrintStream stream, int num) {
        IntStream.range(0, num).forEach(ignored -> stream.print("#"));
    }

    private void writeConfigKeyGroup(PrintStream stream, ConfigKeyGroup group) {
        int maxLength = Math.min(
            group.description().length() + COMMENT_PREFIX.length(),
            MAX_COLUMN_SIZE
        );
        // "###########"
        writeNSharps(stream, maxLength);
        stream.println();
        // "# Settings of `<group>`
        writeSentence(stream, COMMENT_PREFIX, "Settings of `" + group.name() + "`");
        stream.println("#");
        // "# <group description>"
        writeSentence(stream, COMMENT_PREFIX, group.description());
        // "###########"
        writeNSharps(stream, maxLength);
        stream.println();
    }

    private void writeConfigKey(PrintStream stream,
                                ConfigKey key) {
        // "# <description>"
        // "#"
        if (StringUtils.isNotBlank(key.description())) {
            writeSentence(stream, COMMENT_PREFIX, key.description());
            stream.println("#");
        }
        // "# <documentation>"
        // "#"
        if (StringUtils.isNotBlank(key.documentation())) {
            writeSentence(stream, COMMENT_PREFIX, key.documentation());
            stream.println("#");
        }
        // "# type: <type>, required"
        writeSentence(
            stream,
            COMMENT_PREFIX,
            "TYPE: " + key.type() + ", " + (key.required() ? "required" : "optional"));
        if (null != key.validator() && StringUtils.isNotBlank(key.validator().documentation())) {
            writeSentence(
                stream, COMMENT_PREFIX,
                "@constraints : " + key.validator().documentation()
            );
        }
        if (!key.optionValues().isEmpty()) {
            writeSentence(
                stream, COMMENT_PREFIX, "@options :"
            );
            key.optionValues().forEach(value -> {
                writeSentence(
                    stream, COMMENT_PREFIX, "  " + value
                );
            });
        }
        // "#"
        // "# @Since"
        if (StringUtils.isNotBlank(key.since())) {
            stream.println("#");
            writeSentence(stream, COMMENT_PREFIX,
                "@since " + key.since() + "");
        }
        // "#"
        // "# @Deprecated"
        if (key.deprecated()) {
            stream.println("#");
            writeSentence(stream, COMMENT_PREFIX, getDeprecationDescription(key));
        }
        // <key>=<defaultValue>
        stream.print(key.name());
        stream.print("=");
        if (null != key.defaultValue()) {
            stream.print(key.defaultValue());
        }
        stream.println();
    }

    private String getDeprecationDescription(ConfigKey key) {
        StringBuilder sb = new StringBuilder();
        sb.append("@deprecated");
        if (StringUtils.isNotBlank(key.deprecatedSince())) {
            sb.append(" since `")
              .append(key.deprecatedSince())
              .append("`");
        }
        if (StringUtils.isNotBlank(key.deprecatedByConfigKey())) {
            sb.append(" in favor of using `")
              .append(key.deprecatedByConfigKey())
              .append("`");
        }
        return sb.toString();
    }

    private void writeSentence(PrintStream stream,
                               String prefix,
                               String sentence) {
        int max = MAX_COLUMN_SIZE;
        String[] words = sentence.split(" ");
        int i = 0;
        stream.print(prefix);
        int current = prefix.length();
        while (i < words.length) {
            String word = words[i];
            if (word.length() > max || current + word.length() <= max) {
                if (i != 0) {
                    stream.print(" ");
                }
                stream.print(word);
                current += (word.length() + 1);
            } else {
                stream.println();
                stream.print(prefix);
                stream.print(word);
                current = prefix.length() + word.length();
            }
            ++i;
        }
        stream.println();
    }

    private void save(PrintStream stream) {
        for (ConfigKeyGroup group : groups) {
            writeConfigKeyGroup(stream, group);
            stream.println();
            Set<ConfigKey> groupKeys = settings.getOrDefault(group.name(), Collections.emptySet());
            groupKeys.forEach(key -> {
                writeConfigKey(stream, key);
                stream.println();
            });
        }
    }


}
