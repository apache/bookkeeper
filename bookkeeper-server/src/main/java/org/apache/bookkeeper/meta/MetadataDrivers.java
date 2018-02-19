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
package org.apache.bookkeeper.meta;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * A driver manager for managing a set of metadata drivers.
 *
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class MetadataDrivers {

    static final String ZK_CLIENT_DRIVER_CLASS = "org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver";
    static final String ZK_BOOKIE_DRIVER_CLASS = "org.apache.bookkeeper.meta.zk.ZKMetadataBookieDriver";
    static final String BK_METADATA_CLIENT_DRIVERS_PROPERTY = "bookkeeper.metadata.client.drivers";
    static final String BK_METADATA_BOOKIE_DRIVERS_PROPERTY = "bookkeeper.metadata.bookie.drivers";

    @ToString
    static class MetadataClientDriverInfo {

        final Class<? extends MetadataClientDriver> driverClass;
        final String driverClassName;

        MetadataClientDriverInfo(Class<? extends MetadataClientDriver> driverClass) {
            this.driverClass = driverClass;
            this.driverClassName = driverClass.getName();
        }

    }

    @ToString
    static class MetadataBookieDriverInfo {

        final Class<? extends MetadataBookieDriver> driverClass;
        final String driverClassName;

        MetadataBookieDriverInfo(Class<? extends MetadataBookieDriver> driverClass) {
            this.driverClass = driverClass;
            this.driverClassName = driverClass.getName();
        }

    }

    @Getter(AccessLevel.PACKAGE)
    private static final ConcurrentMap<String, MetadataClientDriverInfo> clientDrivers;
    @Getter(AccessLevel.PACKAGE)
    private static final ConcurrentMap<String, MetadataBookieDriverInfo> bookieDrivers;
    private static boolean initialized = false;

    static {
        clientDrivers = new ConcurrentHashMap<>();
        bookieDrivers = new ConcurrentHashMap<>();
        initialize();
    }

    static void initialize() {
        if (initialized) {
            return;
        }
        loadInitialDrivers();
        initialized = true;
        log.info("BookKeeper metadata driver manager initialized");
    }

    @VisibleForTesting
    static void loadInitialDrivers() {
        loadInitialClientDrivers();
        loadInitialBookieDrivers();
    }

    private static void loadInitialClientDrivers() {
        Set<String> driverList = Sets.newHashSet();

        // add default zookeeper based driver
        driverList.add(ZK_CLIENT_DRIVER_CLASS);

        // load drivers from system property
        String driversStr = System.getProperty(BK_METADATA_CLIENT_DRIVERS_PROPERTY);
        if (null != driversStr) {
            String[] driversArray = StringUtils.split(driversStr, ':');
            for (String driver : driversArray) {
                driverList.add(driver);
            }
        }

        // initialize the drivers
        for (String driverClsName : driverList) {
            try {
                MetadataClientDriver driver =
                    ReflectionUtils.newInstance(driverClsName, MetadataClientDriver.class);
                MetadataClientDriverInfo driverInfo =
                    new MetadataClientDriverInfo(driver.getClass());
                clientDrivers.put(driver.getScheme().toLowerCase(), driverInfo);
            } catch (Exception e) {
                log.warn("Failed to load metadata client driver {}", driverClsName, e);
            }
        }
    }

    private static void loadInitialBookieDrivers() {
        Set<String> driverList = Sets.newHashSet();

        // add default zookeeper based driver
        driverList.add(ZK_BOOKIE_DRIVER_CLASS);

        // load drivers from system property
        String driversStr = System.getProperty(BK_METADATA_BOOKIE_DRIVERS_PROPERTY);
        if (null != driversStr) {
            String[] driversArray = StringUtils.split(driversStr, ':');
            for (String driver : driversArray) {
                driverList.add(driver);
            }
        }

        // initialize the drivers
        for (String driverClsName : driverList) {
            try {
                MetadataBookieDriver driver =
                    ReflectionUtils.newInstance(driverClsName, MetadataBookieDriver.class);
                MetadataBookieDriverInfo driverInfo =
                    new MetadataBookieDriverInfo(driver.getClass());
                bookieDrivers.put(driver.getScheme().toLowerCase(), driverInfo);
            } catch (Exception e) {
                log.warn("Failed to load metadata bookie driver {}", driverClsName, e);
            }
        }
    }

    /**
     * Register the metadata client {@code driver}.
     *
     * @param metadataBackendScheme scheme of metadata backend.
     * @param driver metadata client driver
     */
    public static void registerClientDriver(String metadataBackendScheme,
                                            Class<? extends MetadataClientDriver> driver) {
        registerClientDriver(metadataBackendScheme, driver, false);
    }

    @VisibleForTesting
    public static void registerClientDriver(String metadataBackendScheme,
                                            Class<? extends MetadataClientDriver> driver,
                                            boolean allowOverride) {
        if (!initialized) {
            initialize();
        }

        String scheme = metadataBackendScheme.toLowerCase();
        MetadataClientDriverInfo oldDriverInfo = clientDrivers.get(scheme);
        if (null != oldDriverInfo && !allowOverride) {
            return;
        }
        MetadataClientDriverInfo newDriverInfo = new MetadataClientDriverInfo(driver);
        if (allowOverride) {
            oldDriverInfo = clientDrivers.put(scheme, newDriverInfo);
        } else {
            oldDriverInfo = clientDrivers.putIfAbsent(scheme, newDriverInfo);
        }
        if (null != oldDriverInfo) {
            log.debug("Metadata client driver for {} is already there.", scheme);
        }
    }

    /**
     * Register the metadata bookie {@code driver}.
     *
     * @param metadataBackendScheme scheme of metadata backend.
     * @param driver metadata bookie driver
     */
    public static void registerBookieDriver(String metadataBackendScheme,
                                            Class<? extends MetadataBookieDriver> driver) {
        registerBookieDriver(metadataBackendScheme, driver, false);
    }

    @VisibleForTesting
    public static void registerBookieDriver(String metadataBackendScheme,
                                            Class<? extends MetadataBookieDriver> driver,
                                            boolean allowOverride) {
        if (!initialized) {
            initialize();
        }

        String scheme = metadataBackendScheme.toLowerCase();
        MetadataBookieDriverInfo oldDriverInfo = bookieDrivers.get(scheme);
        if (null != oldDriverInfo && !allowOverride) {
            return;
        }
        MetadataBookieDriverInfo newDriverInfo = new MetadataBookieDriverInfo(driver);
        if (allowOverride) {
            oldDriverInfo = bookieDrivers.put(scheme, newDriverInfo);
        } else {
            oldDriverInfo = bookieDrivers.putIfAbsent(scheme, newDriverInfo);
        }
        if (null != oldDriverInfo) {
            log.debug("Metadata bookie driver for {} is already there.", scheme);
        }
    }

    /**
     * Retrieve the client driver for {@code scheme}.
     *
     * @param scheme the scheme for the metadata client driver
     * @return the metadata client driver
     * @throws NullPointerException when scheme is null
     */
    public static MetadataClientDriver getClientDriver(String scheme) {
        checkNotNull(scheme, "Client Driver Scheme is null");
        if (!initialized) {
            initialize();
        }
        MetadataClientDriverInfo driverInfo = clientDrivers.get(scheme.toLowerCase());
        if (null == driverInfo) {
            throw new IllegalArgumentException("Unknown backend " + scheme);
        }
        return ReflectionUtils.newInstance(driverInfo.driverClass);
    }

    /**
     * Retrieve the client driver for {@code uri}.
     *
     * @param uri the metadata service uri
     * @return the metadata client driver for {@code uri}
     * @throws NullPointerException if the metadata service {@code uri} is null or doesn't have scheme
     *          or there is no namespace driver registered for the scheme
     * @throws IllegalArgumentException if the metadata service {@code uri} scheme is illegal
     */
    public static MetadataClientDriver getClientDriver(URI uri) {
        // Validate the uri and load the backend according to scheme
        checkNotNull(uri, "Metadata service uri is null");
        String scheme = uri.getScheme();
        checkNotNull(scheme, "Invalid metadata service uri : " + uri);
        scheme = scheme.toLowerCase();
        String[] schemeParts = StringUtils.split(scheme, '+');
        checkArgument(schemeParts.length > 0,
                "Invalid metadata service scheme found : " + uri);
        return getClientDriver(schemeParts[0]);
    }

    /**
     * Retrieve the bookie driver for {@code scheme}.
     *
     * @param scheme the scheme for the metadata bookie driver
     * @return the metadata bookie driver
     * @throws NullPointerException when scheme is null
     */
    public static MetadataBookieDriver getBookieDriver(String scheme) {
        checkNotNull(scheme, "Bookie Driver Scheme is null");
        if (!initialized) {
            initialize();
        }
        MetadataBookieDriverInfo driverInfo = bookieDrivers.get(scheme.toLowerCase());
        if (null == driverInfo) {
            throw new IllegalArgumentException("Unknown backend " + scheme);
        }
        return ReflectionUtils.newInstance(driverInfo.driverClass);
    }

    /**
     * Retrieve the bookie driver for {@code uri}.
     *
     * @param uri the metadata service uri
     * @return the metadata bookie driver for {@code uri}
     * @throws NullPointerException if the metadata service {@code uri} is null or doesn't have scheme
     *          or there is no namespace driver registered for the scheme
     * @throws IllegalArgumentException if the metadata service {@code uri} scheme is illegal
     */
    public static MetadataBookieDriver getBookieDriver(URI uri) {
        // Validate the uri and load the backend according to scheme
        checkNotNull(uri, "Metadata service uri is null");
        String scheme = uri.getScheme();
        checkNotNull(scheme, "Invalid metadata service uri : " + uri);
        scheme = scheme.toLowerCase();
        String[] schemeParts = StringUtils.split(scheme, '+');
        checkArgument(schemeParts.length > 0,
                "Invalid metadata service scheme found : " + uri);
        return getBookieDriver(schemeParts[0]);
    }

}
