/**
 *
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
 *
 */
package org.apache.bookkeeper.stats;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An umbrella class for loading stats provider.
 */
public class Stats {
    static final Logger LOG = LoggerFactory.getLogger(Stats.class);
    public static final String STATS_PROVIDER_CLASS = "statsProviderClass";

    static StatsProvider prov = new NullStatsProvider();

    public static void loadStatsProvider(Configuration conf) {
        String className = conf.getString(STATS_PROVIDER_CLASS);
        loadStatsProvider(className);
    }

    public static void loadStatsProvider(String className) {
        if (className != null) {
            try {
                Class cls = Class.forName(className);
                @SuppressWarnings("unchecked")
                Constructor<? extends StatsProvider> cons =
                    (Constructor<? extends StatsProvider>) cls.getDeclaredConstructor();
                prov = cons.newInstance();
            } catch (ClassNotFoundException cnfe) {
                LOG.error("Couldn't find configured class(" + className + ")", cnfe);
            } catch (NoSuchMethodException nsme) {
                LOG.error("Couldn't find default constructor for class (" + className + ")", nsme);
            } catch (InstantiationException ie) {
                LOG.error("Couldn't construct class (" + className + ")", ie);
            } catch (IllegalAccessException iae) {
                LOG.error("Couldn't construct class (" + className + "),"
                          + " Is the constructor private?", iae);
            } catch (InvocationTargetException ite) {
                LOG.error("Constructor threw an exception. It should not have.", ite);
            }
        }
    }

    public static StatsProvider get() {
        return prov;
    }
}
