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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.conf.validators.NullValidator;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

/**
 * A configuration key in a configuration.
 */
@Data
@Builder(builderMethodName = "internalBuilder")
@Accessors(fluent = true)
@Public
@Slf4j
public class ConfigKey {

    public static final Comparator<ConfigKey> ORDERING = (o1, o2) -> {
        int ret = Integer.compare(o1.orderInGroup, o2.orderInGroup);
        if (ret == 0) {
            return o1.name().compareTo(o2.name());
        } else {
            return ret;
        }
    };

    /**
     * Build a config key of <tt>name</tt>.
     *
     * @param name config key name
     * @return config key builder
     */
    public static ConfigKeyBuilder builder(String name) {
        return internalBuilder().name(name);
    }

    /**
     * Flag indicates whether the setting is required.
     */
    @Default
    private boolean required = false;

    /**
     * Name of the configuration setting.
     */
    private String name;

    /**
     * Type of the configuration setting.
     */
    @Default
    private Type type = Type.STRING;

    /**
     * Description of the configuration setting.
     */
    @Default
    private String description = "";

    /**
     * Documentation of the configuration setting.
     */
    @Default
    private String documentation = "";

    /**
     * Default value as a string representation.
     */
    @Default
    private Object defaultValue = null;

    private String defaultValueAsString() {
        if (null == defaultValue) {
            return null;
        } else if (defaultValue instanceof String) {
            return (String) defaultValue;
        } else if (defaultValue instanceof Class) {
            return ((Class) defaultValue).getName();
        } else {
            return defaultValue.toString();
        }
    }

    /**
     * The list of options for this setting.
     */
    @Default
    private List<String> optionValues = Collections.emptyList();

    /**
     * The validator used for validating configuration value.
     */
    @Default
    private Validator validator = NullValidator.of();

    /**
     * The key-group to group settings together.
     */
    @Default
    private ConfigKeyGroup group = ConfigKeyGroup.DEFAULT;

    /**
     * The order of the setting in the key-group.
     */
    @Default
    private int orderInGroup = Integer.MIN_VALUE;

    /**
     * The list of settings dependents on this setting.
     */
    @Default
    private List<String> dependents = Collections.emptyList();

    /**
     * Whether this setting is deprecated or not.
     */
    @Default
    private boolean deprecated = false;

    /**
     * The config key that deprecates this key.
     */
    @Default
    private String deprecatedByConfigKey = "";

    /**
     * The version when this settings was deprecated.
     */
    @Default
    private String deprecatedSince = "";

    /**
     * The version when this setting was introduced.
     */
    @Default
    private String since = "";

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ConfigKey)) {
            return false;
        }
        ConfigKey other = (ConfigKey) o;
        return Objects.equals(name, other.name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * Validate the setting is valid in the provided config <tt>conf</tt>.
     *
     * @param conf configuration to test
     */
    public void validate(Configuration conf) throws ConfigException {
        if (conf.containsKey(name()) && validator() != null) {
            Object value = get(conf);
            if (!validator().validate(name(), value)) {
                throw new ConfigException("Invalid setting of '" + name()
                    + "' found the configuration: value = '" + value + "', requirement = '" + validator + "'");
            }
        } else if (required()) { // missing config on a required field
            throw new ConfigException(
                "Setting '" + name() + "' is required but missing in the configuration");
        }
    }

    /**
     * Update the setting <tt>name</tt> in the configuration <tt>conf</tt> with the provided <tt>value</tt>.
     *
     * @param conf configuration to set
     * @param value value of the setting
     */
    public void set(Configuration conf, Object value) {
        if (!type().validator().validate(name(), value)) {
            throw new IllegalArgumentException(
                "Invalid value '" + value + "' to set on setting '" + name() + "': expected type = " + type);
        }

        if (null != validator() && !validator().validate(name(), value)) {
            throw new IllegalArgumentException(
                "Invalid value '" + value + "' to set on setting '" + name() + "': required '" + validator() + "'");
        }

        if (value instanceof Class) {
            conf.setProperty(name(), ((Class) value).getName());
        } else {
            conf.setProperty(name(), value);
        }
    }

    /**
     * Retrieve the setting from the configuration <tt>conf</tt> as a {@link Long} value.
     *
     * @param conf configuration to retrieve the setting
     * @return the value as a long number
     */
    public long getLong(Configuration conf) {
        checkArgument(type() == Type.LONG, "'" + name() + "' is NOT a LONG numeric setting");
        return conf.getLong(name(), (Long) defaultValue());
    }

    /**
     * Retrieve the setting from the configuration <tt>conf</tt> as a {@link Integer} value.
     *
     * @param conf configuration to retrieve the setting
     * @return the value as an integer number
     */
    public int getInt(Configuration conf) {
        checkArgument(type() == Type.INT, "'" + name() + "' is NOT a INT numeric setting");
        return conf.getInt(name(), (Integer) defaultValue());
    }

    /**
     * Retrieve the setting from the configuration <tt>conf</tt> as a {@link Short} value.
     *
     * @param conf configuration to retrieve the setting
     * @return the value as a short number
     */
    public short getShort(Configuration conf) {
        checkArgument(type() == Type.SHORT, "'" + name() + "' is NOT a SHORT numeric setting");
        return conf.getShort(name(), (Short) defaultValue());
    }

    /**
     * Retrieve the setting from the configuration <tt>conf</tt> as a {@link Boolean} value.
     *
     * @param conf configuration to retrieve the setting
     * @return the value as a boolean flag
     */
    public boolean getBoolean(Configuration conf) {
        checkArgument(type() == Type.BOOLEAN, "'" + name() + "' is NOT a BOOL numeric setting");
        return conf.getBoolean(name(), (Boolean) defaultValue());
    }

    /**
     * Retrieve the setting from the configuration <tt>conf</tt> as a {@link Double} value.
     *
     * @param conf configuration to retrieve the setting
     * @return the value as a double number
     */
    public double getDouble(Configuration conf) {
        checkArgument(type() == Type.DOUBLE, "'" + name() + "' is NOT a DOUBLE numeric setting");
        return conf.getDouble(name(), (Double) defaultValue());
    }

    /**
     * Retrieve the setting from the configuration <tt>conf</tt> as a {@link String} value.
     *
     * @param conf configuration to retrieve the setting
     * @return the value as a string.
     */
    public String getString(Configuration conf) {
        return conf.getString(name(), defaultValueAsString());
    }

    /**
     * Retrieve the setting from the configuration <tt>conf</tt> as a {@link Class} value.
     *
     * @param conf configuration to retrieve the setting
     * @return the value as a class
     */
    @SuppressWarnings("unchecked")
    public <T> Class<? extends T> getClass(Configuration conf, Class<T> interfaceCls) {
        checkArgument(type() == Type.CLASS, "'" + name() + "' is NOT a CLASS setting");
        try {
            Class<? extends T> defaultClass = (Class<? extends T>) defaultValue();
            return ReflectionUtils.getClass(conf, name(), defaultClass, interfaceCls, getClass().getClassLoader());
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException("Invalid class is set to setting '" + name() + "': ", e);
        }
    }

    /**
     * Retrieve the setting from the configuration <tt>conf</tt> as a {@link Class} value.
     *
     * @param conf configuration to retrieve the setting
     * @return the value as a class
     */
    @SuppressWarnings("unchecked")
    public Class<?> getClass(Configuration conf) {
        checkArgument(type() == Type.CLASS, "'" + name() + "' is NOT a CLASS setting");
        try {
            Class<?> defaultClass = (Class<?>) defaultValue();
            return ReflectionUtils.getClass(conf, name(), defaultClass, getClass().getClassLoader());
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException("Invalid class is set to setting '" + name() + "': ", e);
        }
    }

    /**
     * Retrieve the setting from the configuration <tt>conf</tt> as a {@link Class} value.
     *
     * @param conf configuration to retrieve the setting
     * @return the value as list of values
     */
    @SuppressWarnings("unchecked")
    public List<Object> getList(Configuration conf) {
        checkArgument(type() == Type.LIST, "'" + name() + "' is NOT a LIST setting");
        List<Object> list = (List<Object>) defaultValue();
        if (null == list) {
            list = Collections.emptyList();
        }
        return conf.getList(name(), list);
    }

    /**
     * Retrieve the setting value from the provided <tt>conf</tt>.
     *
     * @return the setting value
     */
    public Object get(Configuration conf) {
        switch (type()) {
            case LONG:
                return getLong(conf);
            case INT:
                return getInt(conf);
            case SHORT:
                return getShort(conf);
            case DOUBLE:
                return getDouble(conf);
            case BOOLEAN:
                return getBoolean(conf);
            case LIST:
                return getList(conf);
            case CLASS:
                return getClass(conf);
            default:
                return getString(conf);
        }
    }
}
