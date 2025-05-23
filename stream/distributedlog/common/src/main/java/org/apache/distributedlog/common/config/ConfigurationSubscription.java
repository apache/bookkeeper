/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.common.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.ConfigurationBuilderEvent;
import org.apache.commons.configuration2.builder.ReloadingFileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.FileBasedBuilderParameters;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.reloading.PeriodicReloadingTrigger;
import org.apache.commons.configuration2.reloading.ReloadingController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConfigurationSubscription publishes a reloading, thread-safe view of file configuration. The class
 * periodically calls FileConfiguration.reload on the underlying conf, and propagates changes to the
 * concurrent config. The configured FileChangedReloadingStrategy ensures that file config will only
 * be reloaded if something changed.
 * Notes:
 * 1. Reload schedule is never terminated. The assumption is a finite number of these are started
 * at the calling layer, and terminated only once the executor service is shut down.
 * 2. The underlying FileConfiguration is not at all thread-safe, so its important to ensure access
 * to this object is always single threaded.
 */
public class ConfigurationSubscription implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(ConfigurationSubscription.class);

    private final ConcurrentBaseConfiguration viewConfig;
    private final ScheduledExecutorService executorService;
    private final int reloadPeriod;
    private final TimeUnit reloadUnit;
    private final Map<File, FileBasedConfiguration> fileConfigs;
    private final CopyOnWriteArraySet<ConfigurationListener> confListeners;
    private final Map<File, PeriodicReloadingTrigger> reloadingTriggers;

    public ConfigurationSubscription(ConcurrentBaseConfiguration viewConfig,
                                     List<File> configFiles,
                                     ScheduledExecutorService executorService,
                                     int reloadPeriod,
                                     TimeUnit reloadUnit)
            throws ConfigurationException {
        checkNotNull(configFiles);
        checkArgument(!configFiles.isEmpty());
        checkNotNull(executorService);
        checkNotNull(viewConfig);
        this.viewConfig = viewConfig;
        this.executorService = executorService;
        this.reloadPeriod = reloadPeriod;
        this.reloadUnit = reloadUnit;
        this.reloadingTriggers = new java.util.HashMap<>();
        this.fileConfigs = new LinkedHashMap<>();
        this.confListeners = new CopyOnWriteArraySet<>();
        initConfig(configFiles);
        refreshConfiguration();
    }

    public void registerListener(ConfigurationListener listener) {
        this.confListeners.add(listener);
    }

    public void unregisterListener(ConfigurationListener listener) {
        this.confListeners.remove(listener);
    }

    private void initConfig(List<File> configFiles) {
        try {
            for (File configFile : configFiles) {
                FileBasedBuilderParameters parameters =
                        new Parameters()
                                .fileBased()
                                .setFile(configFile)
                                .setReloadingRefreshDelay(0L)
                                .setListDelimiterHandler(new DefaultListDelimiterHandler(','));
                ReloadingFileBasedConfigurationBuilder<PropertiesConfiguration> configBuilder =
                        new ReloadingFileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                                .configure(parameters);
                ReloadingController reloadingController = configBuilder.getReloadingController();
                PeriodicReloadingTrigger trigger =
                        new PeriodicReloadingTrigger(reloadingController,
                                configFile, reloadPeriod, reloadUnit, executorService);
                trigger.start();
                reloadingTriggers.put(configFile, trigger);
                configBuilder.addEventListener(ConfigurationBuilderEvent.RESET, event -> {
                    try {
                        configChanged(configFile, event);
                    } catch (ConfigurationException e) {
                        LOG.error("Config reload failed for file {}", configFile, e);
                    }
                });
                FileBasedConfiguration fileConfig = configBuilder.getConfiguration();
                fileConfigs.put(configFile, fileConfig);
            }
        } catch (ConfigurationException ex) {
            if (!fileNotFound(ex)) {
                LOG.error("Config init failed", ex);
            }
        }
    }

    void configChanged(File configFile, ConfigurationBuilderEvent event) throws ConfigurationException {
        ReloadingFileBasedConfigurationBuilder<PropertiesConfiguration> configBuilder =
                (ReloadingFileBasedConfigurationBuilder<PropertiesConfiguration>) event.getSource();
        FileBasedConfiguration configuration = configBuilder.getConfiguration();
        fileConfigs.put(configFile, configuration);
        refreshConfiguration();
    }

    synchronized void refreshConfiguration() {
        // Reload if config exists.
        Set<String> confKeys = Sets.newHashSet();
        for (Map.Entry<File, FileBasedConfiguration> entry : fileConfigs.entrySet()) {
            File configFile = entry.getKey();
            FileBasedConfiguration fileConfig = entry.getValue();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Check and reload config, file={}, lastModified={}", configFile,
                        configFile.lastModified());
            }
            // load keys
            Iterator keyIter = fileConfig.getKeys();
            while (keyIter.hasNext()) {
                String key = (String) keyIter.next();
                confKeys.add(key);
            }
        }
        // clear unexisted keys
        Iterator viewIter = viewConfig.getKeys();
        while (viewIter.hasNext()) {
            String key = (String) viewIter.next();
            if (!confKeys.contains(key)) {
                clearViewProperty(key);
            }
        }
        LOG.info("Reload features : {}", confKeys);
        // load keys from files
        for (Map.Entry<File, FileBasedConfiguration> entry : fileConfigs.entrySet()) {
            File configFile = entry.getKey();
            FileBasedConfiguration fileConfig = entry.getValue();
            try {
                loadView(fileConfig);
            } catch (Exception ex) {
                if (!fileNotFound(ex)) {
                    LOG.error("Config reload failed for file {}", configFile, ex);
                }
            }
        }
        for (ConfigurationListener listener : confListeners) {
            listener.onReload(viewConfig);
        }
    }

    private boolean fileNotFound(Exception ex) {
        return ex instanceof FileNotFoundException
            || ex.getCause() != null && ex.getCause() instanceof FileNotFoundException;
    }

    private void loadView(FileBasedConfiguration fileConfig) {
        Iterator fileIter = fileConfig.getKeys();
        while (fileIter.hasNext()) {
            String key = (String) fileIter.next();
            setViewProperty(fileConfig, key, fileConfig.getProperty(key));
        }
    }

    private void clearViewProperty(String key) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing property, key={}", key);
        }
        viewConfig.clearProperty(key);
    }

    private void setViewProperty(FileBasedConfiguration fileConfig,
                                 String key,
                                 Object value) {
        if (!viewConfig.containsKey(key) || !viewConfig.getProperty(key).equals(value)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Setting property, key={} value={}", key, fileConfig.getProperty(key));
            }
            viewConfig.setProperty(key, fileConfig.getProperty(key));
        }
    }

    @Override
    public void close() throws Exception {
        reloadingTriggers.values().forEach(PeriodicReloadingTrigger::stop);
    }
}
