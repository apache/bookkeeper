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

import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * A configuration key in a configuration
 */
@Data
@Builder
@Accessors(fluent = true)
public class ConfigKey {

    /**
     * Name of the configuration setting.
     */
    private final String name;

    /**
     * Type of the configuration setting.
     */
    private final Type type;

    /**
     * Documentation of the configuration setting.
     */
    private final String documentation;

    /**
     * Default value as a string representation.
     */
    private final Object defaultValue;

    /**
     * The validator used for validating configuration value.
     */
    private final Validator validator;

    /**
     * The category to group settings together.
     */
    private final String category;

    /**
     * The order of the setting in the category.
     */
    private final int orderInCategory;

    /**
     * The list of settings dependents on this setting.
     */
    private final List<String> dependents;

    /**
     * Whether this setting is deprecated or not.
     */
    private final boolean deprecated;

    /**
     * The config key that deprecates this key.
     */
    private final String deprecatedByConfigKey;

}
