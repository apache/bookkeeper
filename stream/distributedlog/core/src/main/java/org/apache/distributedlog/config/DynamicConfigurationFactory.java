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
package org.apache.distributedlog.config;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.common.config.ConcurrentBaseConfiguration;
import org.apache.distributedlog.common.config.ConcurrentConstConfiguration;
import org.apache.distributedlog.common.config.ConfigurationSubscription;

/**
 * Encapsulates creation of DynamicDistributedLogConfiguration instances. Ensures one instance per
 * factory.
 * Notes:
 * Once loaded, stays loaded until shutdown. Caller ensures small finite number of configs are created.
 */
@CustomLog
public class DynamicConfigurationFactory {

    private final Map<String, DynamicDistributedLogConfiguration> dynamicConfigs;
    private final List<ConfigurationSubscription> subscriptions;
    private final ScheduledExecutorService executorService;
    private final int reloadPeriod;
    private final TimeUnit reloadUnit;

    public DynamicConfigurationFactory(ScheduledExecutorService executorService,
                                       int reloadPeriod, TimeUnit reloadUnit) {
        this.executorService = executorService;
        this.reloadPeriod = reloadPeriod;
        this.reloadUnit = reloadUnit;
        this.dynamicConfigs = new HashMap<>();
        this.subscriptions = new LinkedList<>();
    }

    public synchronized Optional<DynamicDistributedLogConfiguration> getDynamicConfiguration(
            String configPath,
            ConcurrentBaseConfiguration defaultConf) throws ConfigurationException {
        checkNotNull(configPath);
        if (!dynamicConfigs.containsKey(configPath)) {
            File configFile = new File(configPath);
            DynamicDistributedLogConfiguration dynConf =
                    new DynamicDistributedLogConfiguration(defaultConf);
            ConfigurationSubscription subscription = new ConfigurationSubscription(
                    dynConf, Collections.singletonList(configFile), executorService, reloadPeriod, reloadUnit);
            subscriptions.add(subscription);
            dynamicConfigs.put(configPath, dynConf);
            log.info().attr("configPath", configPath).log("Loaded dynamic configuration");
        }
        return Optional.of(dynamicConfigs.get(configPath));
    }

    public synchronized Optional<DynamicDistributedLogConfiguration>
    getDynamicConfiguration(String configPath) throws ConfigurationException {
        return getDynamicConfiguration(configPath, new ConcurrentConstConfiguration(new DistributedLogConfiguration()));
    }
}
