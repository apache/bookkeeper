/**
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
package org.apache.distributedlog.namespace;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.distributedlog.DistributedLogConstants;
import org.apache.distributedlog.impl.BKNamespaceDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * The basic service for managing a set of namespace drivers.
 */
public class NamespaceDriverManager {

    private static final Logger logger = LoggerFactory.getLogger(NamespaceDriverManager.class);

    static class NamespaceDriverInfo {

        final Class<? extends NamespaceDriver> driverClass;
        final String driverClassName;

        NamespaceDriverInfo(Class<? extends NamespaceDriver> driverClass) {
            this.driverClass = driverClass;
            this.driverClassName = this.driverClass.getName();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("driver[")
                    .append(driverClassName)
                    .append("]");
            return sb.toString();
        }
    }

    private static final ConcurrentMap<String, NamespaceDriverInfo> drivers;
    private static boolean initialized = false;

    static {
        drivers = new ConcurrentHashMap<String, NamespaceDriverInfo>();
        initialize();
    }

    static void initialize() {
        if (initialized) {
            return;
        }
        loadInitialDrivers();
        initialized = true;
        logger.info("DistributedLog NamespaceDriverManager initialized");
    }

    private static void loadInitialDrivers() {
        Set<String> driverList = Sets.newHashSet();
        // add default bookkeeper based driver
        driverList.add(BKNamespaceDriver.class.getName());
        // load drivers from system property
        String driversStr = System.getProperty("distributedlog.namespace.drivers");
        if (null != driversStr) {
            String[] driversArray = StringUtils.split(driversStr, ':');
            driverList.addAll(Arrays.asList(driversArray));
        }
        // initialize the drivers
        for (String driverClsName : driverList) {
            try {
                NamespaceDriver driver =
                        ReflectionUtils.newInstance(driverClsName, NamespaceDriver.class);
                NamespaceDriverInfo driverInfo = new NamespaceDriverInfo(driver.getClass());
                drivers.put(driver.getScheme().toLowerCase(), driverInfo);
            } catch (Exception ex) {
                logger.warn("Failed to load namespace driver {} : ", driverClsName, ex);
            }
        }
    }

    /**
     * Prevent the NamespaceDriverManager class from being instantiated.
     */
    private NamespaceDriverManager() {}

    /**
     * Register the namespace {@code driver}.
     *
     * @param driver the namespace driver
     */
    public static void registerDriver(String backend, Class<? extends NamespaceDriver> driver) {
        if (!initialized) {
            initialize();
        }

        String scheme = backend.toLowerCase();
        NamespaceDriverInfo oldDriverInfo = drivers.get(scheme);
        if (null != oldDriverInfo) {
            return;
        }
        NamespaceDriverInfo newDriverInfo = new NamespaceDriverInfo(driver);
        oldDriverInfo = drivers.putIfAbsent(scheme, newDriverInfo);
        if (null != oldDriverInfo) {
            logger.debug("Driver for {} is already there.", scheme);
        }
    }

    /**
     * Retrieve the namespace driver for {@code scheme}.
     *
     * @param scheme the scheme for the namespace driver
     * @return the namespace driver
     * @throws NullPointerException when scheme is null
     */
    public static NamespaceDriver getDriver(String scheme) {
        checkNotNull(scheme, "Driver Scheme is null");
        if (!initialized) {
            initialize();
        }
        NamespaceDriverInfo driverInfo = drivers.get(scheme.toLowerCase());
        if (null == driverInfo) {
            throw new IllegalArgumentException("Unknown backend " + scheme);
        }
        return ReflectionUtils.newInstance(driverInfo.driverClass);
    }

    /**
     * Retrieve the namespace driver for {@code uri}.
     *
     * @param uri the distributedlog uri
     * @return the namespace driver for {@code uri}
     * @throws NullPointerException if the distributedlog {@code uri} is null or doesn't have scheme
     *          or there is no namespace driver registered for the scheme
     * @throws IllegalArgumentException if the distributedlog {@code uri} scheme is illegal
     */
    public static NamespaceDriver getDriver(URI uri) {
        // Validate the uri and load the backend according to scheme
        checkNotNull(uri, "DistributedLog uri is null");
        String scheme = uri.getScheme();
        checkNotNull(scheme, "Invalid distributedlog uri : " + uri);
        scheme = scheme.toLowerCase();
        String[] schemeParts = StringUtils.split(scheme, '-');
        checkArgument(schemeParts.length > 0,
                "Invalid distributedlog scheme found : " + uri);
        checkArgument(Objects.equal(DistributedLogConstants.SCHEME_PREFIX, schemeParts[0].toLowerCase()),
                "Unknown distributedlog scheme found : " + uri);
        // bookkeeper is the default backend
        String backend = DistributedLogConstants.BACKEND_BK;
        if (schemeParts.length > 1) {
            backend = schemeParts[1];
        }
        return getDriver(backend);
    }

}
