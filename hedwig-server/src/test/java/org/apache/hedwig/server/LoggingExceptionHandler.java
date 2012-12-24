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
package org.apache.hedwig.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception handler that simply logs the exception and
 * does nothing more. To be used in tests instead of TerminateJVMExceptionHandler
 */
public class LoggingExceptionHandler implements Thread.UncaughtExceptionHandler {
    static Logger logger = LoggerFactory.getLogger(LoggingExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        logger.error("Uncaught exception in thread " + t.getName(), e);
    }

}
