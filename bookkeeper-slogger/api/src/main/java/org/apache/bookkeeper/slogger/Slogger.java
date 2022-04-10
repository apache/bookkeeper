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
package org.apache.bookkeeper.slogger;

/**
 * Event logging interface will support for key value pairs and reusable context.
 */
public interface Slogger {
    Slogger kv(Object key, Object value);

    Slogger ctx();
    Slogger ctx(Class<?> clazz); // <- should this be class or Logger? Logger requires some generics

    void info(String message);
    void info(String message, Throwable cause);
    void info(Enum<?> event);
    void info(Enum<?> event, Throwable cause);

    void warn(String message);
    void warn(String message, Throwable cause);
    void warn(Enum<?> event);
    void warn(Enum<?> event, Throwable cause);

    void error(String message);
    void error(String message, Throwable cause);
    void error(Enum<?> event);
    void error(Enum<?> event, Throwable cause);

    Slogger NULL = new NullSlogger();
    Slogger CONSOLE = new ConsoleSlogger();
}
