/*
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import lombok.CustomLog;
import org.apache.commons.configuration2.Configuration;

/**
 * An umbrella class for loading stats provider.
 */
@CustomLog
public class Stats {
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
                log.error()
                        .exception(cnfe)
                        .attr("className", className)
                        .log("Couldn't find configured class");
            } catch (NoSuchMethodException nsme) {
                log.error()
                        .exception(nsme)
                        .attr("className", className)
                        .log("Couldn't find default constructor for class");
            } catch (InstantiationException ie) {
                log.error()
                        .exception(ie)
                        .attr("className", className)
                        .log("Couldn't construct class");
            } catch (IllegalAccessException iae) {
                log.error()
                        .exception(iae)
                        .attr("className", className)
                        .log("Couldn't construct class, is the constructor private?");
            } catch (InvocationTargetException ite) {
                log.error().exception(ite).log("Constructor threw an exception. It should not have.");
            }
        }
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public static StatsProvider get() {
        return prov;
    }
}
