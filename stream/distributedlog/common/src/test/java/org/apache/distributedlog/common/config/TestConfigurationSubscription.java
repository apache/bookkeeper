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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.event.ConfigurationEvent;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Notes:
 * 1. lastModified granularity is platform dependent, generally 1 sec, so we can't wait 1ms for things to
 * get picked up.
 */
public class TestConfigurationSubscription {
    static final Logger LOG = LoggerFactory.getLogger(TestConfigurationSubscription.class);
    public static final int RELOAD_PERIOD = 10;

    /**
     * Give some time to handle reloading.
     */
    private void ensureConfigReloaded() throws InterruptedException {
        Thread.sleep(1);
    }

    @Test(timeout = 60000)
    public void testReloadConfiguration() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        ConcurrentConstConfiguration conf = new ConcurrentConstConfiguration(new CompositeConfiguration());
        DeterministicScheduler executorService = new DeterministicScheduler();
        List<File> configFiles = Collections.singletonList(writer.getFile());
        ConfigurationSubscription confSub =
                new ConfigurationSubscription(conf, configFiles, executorService, RELOAD_PERIOD, TimeUnit.MILLISECONDS);
        final AtomicReference<ConcurrentBaseConfiguration> confHolder = new AtomicReference<>();
        confSub.registerListener(new org.apache.distributedlog.common.config.ConfigurationListener() {
            @Override
            public void onReload(ConcurrentBaseConfiguration conf) {
                confHolder.set(conf);
            }
        });
        assertEquals(null, conf.getProperty("prop1"));

        // add
        writer.setProperty("prop1", "1");
        writer.save();
        // ensure the file change reloading event can be triggered
        ensureConfigReloaded();
        executorService.tick(RELOAD_PERIOD, TimeUnit.MILLISECONDS);
        assertNotNull(confHolder.get());
        assertTrue(conf == confHolder.get());
        assertEquals("1", conf.getProperty("prop1"));
    }

    @Test(timeout = 60000)
    public void testAddReloadBasicsConfig() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        DeterministicScheduler mockScheduler = new DeterministicScheduler();
        ConcurrentConstConfiguration conf = new ConcurrentConstConfiguration(new CompositeConfiguration());
        List<File> configFiles = Collections.singletonList(writer.getFile());
        ConfigurationSubscription confSub =
                new ConfigurationSubscription(conf, configFiles, mockScheduler, RELOAD_PERIOD, TimeUnit.MILLISECONDS);
        assertEquals(null, conf.getProperty("prop1"));

        // add
        writer.setProperty("prop1", "1");
        writer.save();
        // ensure the file change reloading event can be triggered
        ensureConfigReloaded();
        mockScheduler.tick(RELOAD_PERIOD, TimeUnit.MILLISECONDS);
        assertEquals("1", conf.getProperty("prop1"));

    }

    @Test(timeout = 60000)
    public void testInitialConfigLoad() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        writer.setProperty("prop1", "1");
        writer.setProperty("prop2", "abc");
        writer.setProperty("prop3", "123.0");
        writer.setProperty("prop4", "11132");
        writer.setProperty("prop5", "true");
        writer.save();

        ScheduledExecutorService mockScheduler = new DeterministicScheduler();
        ConcurrentConstConfiguration conf = new ConcurrentConstConfiguration(new CompositeConfiguration());
        List<File> configFiles = Collections.singletonList(writer.getFile());
        ConfigurationSubscription confSub =
                new ConfigurationSubscription(conf, configFiles, mockScheduler, RELOAD_PERIOD, TimeUnit.MILLISECONDS);
        assertEquals(1, conf.getInt("prop1"));
        assertEquals("abc", conf.getString("prop2"));
        assertEquals(123.0, conf.getFloat("prop3"), 0);
        assertEquals(11132, conf.getInt("prop4"));
        assertEquals(true, conf.getBoolean("prop5"));
    }

    @Test(timeout = 60000)
    public void testExceptionInConfigLoad() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        writer.setProperty("prop1", "1");
        writer.save();

        DeterministicScheduler mockScheduler = new DeterministicScheduler();
        ConcurrentConstConfiguration conf = new ConcurrentConstConfiguration(new CompositeConfiguration());
        List<File> configFiles = Collections.singletonList(writer.getFile());
        ConfigurationSubscription confSub =
                new ConfigurationSubscription(conf, configFiles, mockScheduler, RELOAD_PERIOD, TimeUnit.MILLISECONDS);

        final AtomicInteger count = new AtomicInteger(1);
        conf.addEventListener(ConfigurationEvent.ANY, event -> {
                LOG.info("config changed {}", event);
                // Throw after so we actually see the update anyway.
                if (!event.isBeforeUpdate()) {
                    count.getAndIncrement();
                    throw new RuntimeException("config listener threw and exception");
                }
            });

        int i = 0;
        int initial = 0;
        while (count.get() == initial) {
            writer.setProperty("prop1", Integer.toString(i++));
            writer.save();
            ensureConfigReloaded();
            mockScheduler.tick(RELOAD_PERIOD, TimeUnit.MILLISECONDS);
        }

        initial = count.get();
        while (count.get() == initial) {
            writer.setProperty("prop1", Integer.toString(i++));
            writer.save();
            ensureConfigReloaded();
            mockScheduler.tick(RELOAD_PERIOD, TimeUnit.MILLISECONDS);
        }
    }
}
