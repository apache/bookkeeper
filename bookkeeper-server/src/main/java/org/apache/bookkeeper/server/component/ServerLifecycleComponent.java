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

package org.apache.bookkeeper.server.component;

import com.google.common.collect.Lists;
import java.lang.reflect.Constructor;
import java.util.List;
import lombok.experimental.PackagePrivate;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@link LifecycleComponent} that runs on a bookie server. It can be loaded via reflections.
 */
@PackagePrivate
@Evolving
public abstract class ServerLifecycleComponent extends AbstractLifecycleComponent<BookieConfiguration> {

    public static List<ServerLifecycleComponent> loadServerComponents(String[] componentClassNames,
                                                                      BookieConfiguration conf,
                                                                      StatsLogger statsLogger) {
        List<Class<? extends ServerLifecycleComponent>> componentClasses =
            Lists.newArrayListWithExpectedSize(componentClassNames.length);
        for (String componentClsName : componentClassNames) {
            componentClasses.add(ReflectionUtils.forName(componentClsName, ServerLifecycleComponent.class));
        }
        return Lists.transform(componentClasses, cls -> newComponent(cls, conf, statsLogger));

    }

    static ServerLifecycleComponent newComponent(Class<? extends ServerLifecycleComponent> theCls,
                                                 BookieConfiguration conf,
                                                 StatsLogger statsLogger) {
        try {
            Constructor<? extends ServerLifecycleComponent> constructor =
                theCls.getConstructor(BookieConfiguration.class, StatsLogger.class);
            constructor.setAccessible(true);
            return constructor.newInstance(conf, statsLogger);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected ServerLifecycleComponent(String componentName, BookieConfiguration conf, StatsLogger statsLogger) {
        super(componentName, conf, statsLogger);
    }

}
