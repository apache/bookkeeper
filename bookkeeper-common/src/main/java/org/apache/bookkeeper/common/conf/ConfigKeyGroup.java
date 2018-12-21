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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;

/**
 * Define a group of configuration settings.
 */
@Data
@Accessors(fluent = true)
@Builder(builderMethodName = "internalBuilder")
@Public
public class ConfigKeyGroup {

    /**
     * Ordering the key groups in a configuration.
     */
    public static final Comparator<ConfigKeyGroup> ORDERING = (o1, o2) -> {
        int ret = Integer.compare(o1.order, o2.order);
        if (0 == ret) {
            return o1.name().compareTo(o2.name());
        } else {
            return ret;
        }
    };

    /**
     * Create a config key group of <tt>name</tt>.
     *
     * @param name key group name
     * @return key group builder
     */
    public static ConfigKeyGroupBuilder builder(String name) {
        return internalBuilder().name(name);
    }

    /**
     * The default key group.
     */
    public static final ConfigKeyGroup DEFAULT = builder("").build();

    /**
     * Name of the key group.
     */
    private String name;

    /**
     * Description of the key group.
     */
    @Default
    private String description = "";

    /**
     * The list of sub key-groups of this key group.
     */
    @Default
    private List<String> children = Collections.emptyList();

    /**
     * The order of the key-group in a configuration.
     */
    @Default
    private int order = Integer.MIN_VALUE;

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ConfigKeyGroup)) {
            return false;
        }
        ConfigKeyGroup other = (ConfigKeyGroup) o;
        return Objects.equals(name, other.name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return name.hashCode();
    }

}
