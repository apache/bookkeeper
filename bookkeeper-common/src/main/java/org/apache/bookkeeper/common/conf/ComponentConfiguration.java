/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.conf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.common.util.JsonUtil.ParseJsonException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Component Configuration.
 */
public abstract class ComponentConfiguration implements Configuration {

    protected static final String DELIMITER = ".";

    private final String componentPrefix;
    private final CompositeConfiguration underlyingConf;
    private final Configuration conf;

    protected ComponentConfiguration(CompositeConfiguration underlyingConf,
                                     String componentPrefix) {
        super();
        this.underlyingConf = underlyingConf;
        this.conf = new ConcurrentConfiguration();
        this.componentPrefix = componentPrefix;

        // load the component keys
        loadConf(underlyingConf);
    }

    protected String getKeyName(String name) {
        return name;
    }

    public CompositeConfiguration getUnderlyingConf() {
        return underlyingConf;
    }

    /**
     * Load configuration from a given {@code confURL}.
     *
     * @param confURL the url points to the configuration.
     * @throws ConfigurationException when failed to load configuration.
     */
    public void loadConf(URL confURL) throws ConfigurationException {
        Configuration loadedConf = new PropertiesConfiguration(confURL);
        loadConf(loadedConf);
    }

    protected void loadConf(Configuration loadedConf) {
        loadedConf.getKeys().forEachRemaining(fullKey -> {
            if (fullKey.startsWith(componentPrefix)) {
                String componentKey = fullKey.substring(componentPrefix.length());
                setProperty(componentKey, loadedConf.getProperty(fullKey));
            }
        });
    }

    public void validate() throws ConfigurationException {
        // do nothing by default.
    }

    @Override
    public Configuration subset(String prefix) {
        return conf.subset(getKeyName(prefix));
    }

    @Override
    public boolean isEmpty() {
        return conf.isEmpty();
    }

    @Override
    public boolean containsKey(String key) {
        return conf.containsKey(getKeyName(key));
    }

    @Override
    public void addProperty(String key, Object value) {
        conf.addProperty(getKeyName(key), value);
    }

    @Override
    public void setProperty(String key, Object value) {
        conf.setProperty(getKeyName(key), value);
    }

    @Override
    public void clearProperty(String key) {
        conf.clearProperty(getKeyName(key));
    }

    @Override
    public void clear() {
        conf.clear();
    }

    @Override
    public Object getProperty(String key) {
        return conf.getProperty(getKeyName(key));
    }

    @Override
    public Iterator<String> getKeys(String prefix) {
        return conf.getKeys(getKeyName(prefix));
    }

    @Override
    public Iterator<String> getKeys() {
        return conf.getKeys();
    }

    @Override
    public Properties getProperties(String key) {
        return conf.getProperties(getKeyName(key));
    }

    @Override
    public boolean getBoolean(String key) {
        return conf.getBoolean(getKeyName(key));
    }

    @Override
    public boolean getBoolean(String key, boolean defaultValue) {
        return conf.getBoolean(getKeyName(key), defaultValue);
    }

    @Override
    public Boolean getBoolean(String key, Boolean defaultValue) {
        return conf.getBoolean(getKeyName(key), defaultValue);
    }

    @Override
    public byte getByte(String key) {
        return conf.getByte(getKeyName(key));
    }

    @Override
    public byte getByte(String key, byte defaultValue) {
        return conf.getByte(getKeyName(key), defaultValue);
    }

    @Override
    public Byte getByte(String key, Byte defaultValue) {
        return conf.getByte(getKeyName(key), defaultValue);
    }

    @Override
    public double getDouble(String key) {
        return conf.getDouble(getKeyName(key));
    }

    @Override
    public double getDouble(String key, double defaultValue) {
        return conf.getDouble(getKeyName(key), defaultValue);
    }

    @Override
    public Double getDouble(String key, Double defaultValue) {
        return conf.getDouble(getKeyName(key), defaultValue);
    }

    @Override
    public float getFloat(String key) {
        return conf.getFloat(getKeyName(key));
    }

    @Override
    public float getFloat(String key, float defaultValue) {
        return conf.getFloat(getKeyName(key), defaultValue);
    }

    @Override
    public Float getFloat(String key, Float defaultValue) {
        return conf.getFloat(getKeyName(key), defaultValue);
    }

    @Override
    public int getInt(String key) {
        return conf.getInt(getKeyName(key));
    }

    @Override
    public int getInt(String key, int defaultValue) {
        return conf.getInt(getKeyName(key), defaultValue);
    }

    @Override
    public Integer getInteger(String key, Integer defaultValue) {
        return conf.getInt(getKeyName(key), defaultValue);
    }

    @Override
    public long getLong(String key) {
        return conf.getLong(getKeyName(key));
    }

    @Override
    public long getLong(String key, long defaultValue) {
        return conf.getLong(getKeyName(key), defaultValue);
    }

    @Override
    public Long getLong(String key, Long defaultValue) {
        return conf.getLong(getKeyName(key), defaultValue);
    }

    @Override
    public short getShort(String key) {
        return conf.getShort(getKeyName(key));
    }

    @Override
    public short getShort(String key, short defaultValue) {
        return conf.getShort(getKeyName(key), defaultValue);
    }

    @Override
    public Short getShort(String key, Short defaultValue) {
        return conf.getShort(getKeyName(key), defaultValue);
    }

    @Override
    public BigDecimal getBigDecimal(String key) {
        return conf.getBigDecimal(getKeyName(key));
    }

    @Override
    public BigDecimal getBigDecimal(String key, BigDecimal defaultValue) {
        return conf.getBigDecimal(getKeyName(key), defaultValue);
    }

    @Override
    public BigInteger getBigInteger(String key) {
        return conf.getBigInteger(getKeyName(key));
    }

    @Override
    public BigInteger getBigInteger(String key, BigInteger defaultValue) {
        return conf.getBigInteger(getKeyName(key), defaultValue);
    }

    @Override
    public String getString(String key) {
        return conf.getString(getKeyName(key));
    }

    @Override
    public String getString(String key, String defaultValue) {
        return conf.getString(getKeyName(key), defaultValue);
    }

    @Override
    public String[] getStringArray(String key) {
        return conf.getStringArray(getKeyName(key));
    }

    @Override
    public List<Object> getList(String key) {
        return conf.getList(getKeyName(key));
    }

    @Override
    public List<Object> getList(String key, List<?> defaultValue) {
        return conf.getList(getKeyName(key), defaultValue);
    }

    /**
     * returns the string representation of json format of this config.
     *
     * @return
     * @throws ParseJsonException
     */
    public String asJson() {
        try {
            return JsonUtil.toJson(toMap());
        } catch (ParseJsonException e) {
            throw new RuntimeException("Failed to serialize the configuration as json", e);
        }
    }

    private Map<String, Object> toMap() {
        Map<String, Object> configMap = new HashMap<>();
        Iterator<String> iterator = this.getKeys();
        while (iterator.hasNext()) {
            String key = iterator.next().toString();
            Object property = this.getProperty(key);
            if (property != null) {
                configMap.put(key, property.toString());
            }
        }
        return configMap;
    }
}
