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
package org.apache.bookkeeper.util;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Port manager allows a base port to be specified on the commandline.
 * Tests will then use ports, counting up from this base port.
 * This allows multiple instances of the bookkeeper tests to run at once.
 */
public class PortManager {

    private static final Logger LOG = LoggerFactory.getLogger(PortManager.class);

    private static int nextPort = 15000;

    /**
     * Init the base port.
     *
     * @param initPort initial port
     */
    public static void initPort(int initPort) {
        nextPort = initPort;
    }

    /**
     * Return the available port.
     *
     * @return available port.
     */
    public static synchronized int nextFreePort() {
        int exceptionCount = 0;
        while (true) {
            int port = nextPort++;
            try (ServerSocket ignored = new ServerSocket(port)) {
                // Give it some time to truly close the connection
                TimeUnit.MILLISECONDS.sleep(100);
                return port;
            } catch (IOException ioe) {
                exceptionCount++;
                if (exceptionCount > 100) {
                    throw new RuntimeException("Unable to allocate socket port", ioe);
                }
            } catch (InterruptedException ie) {
                LOG.error("Failed to allocate socket port", ie);
                Thread.currentThread().interrupt();
            }
        }
    }

}
