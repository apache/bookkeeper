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

import static org.apache.bookkeeper.server.component.ServerLifecycleComponent.loadServerComponents;
import static org.apache.bookkeeper.server.component.ServerLifecycleComponent.newComponent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.Test;

/**
 * Manage the test server lifecycle.
 */
public class TestServerLifecycleComponent {

    static class TestComponent extends ServerLifecycleComponent {

        public TestComponent(BookieConfiguration conf, StatsLogger statsLogger) {
            super("test-component", conf, statsLogger);
        }

        @Override
        protected void doStart() {
            // no-op
        }

        @Override
        protected void doStop() {
            // no-op
        }

        @Override
        protected void doClose() throws IOException {
            // no-op
        }
    }

    static class TestComponent2 extends TestComponent {
        public TestComponent2(BookieConfiguration conf, StatsLogger statsLogger) {
            super(conf, statsLogger);
        }
    }

    @Test
    public void testNewComponent() throws Exception {
        BookieConfiguration conf = new BookieConfiguration(new ServerConfiguration());
        StatsLogger statsLogger = NullStatsLogger.INSTANCE;
        ServerLifecycleComponent component = newComponent(
            TestComponent.class,
            conf,
            statsLogger);
        assertEquals("test-component", component.getName());
        assertEquals(conf, component.getConf());
    }

    @Test
    public void testLoadServerComponents() throws Exception {
        BookieConfiguration conf = new BookieConfiguration(new ServerConfiguration());
        StatsLogger statsLogger = NullStatsLogger.INSTANCE;
        String[] clsNames = new String[] {
            TestComponent.class.getName(),
            TestComponent2.class.getName()
        };
        List<ServerLifecycleComponent> components = loadServerComponents(
            clsNames,
            conf,
            statsLogger);
        assertEquals(2, components.size());
        assertTrue(components.get(0) instanceof TestComponent);
        assertTrue(components.get(1) instanceof TestComponent2);
    }


}
