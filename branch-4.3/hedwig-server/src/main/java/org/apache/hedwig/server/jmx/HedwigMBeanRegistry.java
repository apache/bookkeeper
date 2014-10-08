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

package org.apache.hedwig.server.jmx;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.bookkeeper.jmx.BKMBeanRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a unified interface for registering/unregistering of
 * Hedwig MBeans with the platform MBean server.
 */
public class HedwigMBeanRegistry extends BKMBeanRegistry {

    static final String SERVICE = "org.apache.HedwigServer";

    static HedwigMBeanRegistry instance = new HedwigMBeanRegistry();

    public static HedwigMBeanRegistry getInstance(){
        return instance;
    }

    @Override
    protected String getDomainName() {
        return SERVICE;
    }

}
