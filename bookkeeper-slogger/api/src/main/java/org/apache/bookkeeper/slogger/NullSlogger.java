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

class NullSlogger implements Slogger {
    @Override
    public Slogger kv(Object key, Object value) {
        return this;
    }

    @Override
    public Slogger ctx() {
        return this;
    }

    @Override
    public Slogger ctx(Class<?> clazz) {
        return this;
    }

    @Override
    public void info(String message) {}
    @Override
    public void info(String message, Throwable cause) {}
    @Override
    public void info(Enum<?> event) {}
    @Override
    public void info(Enum<?> event, Throwable cause) {}

    @Override
    public void warn(String message) {}
    @Override
    public void warn(String message, Throwable cause) {}
    @Override
    public void warn(Enum<?> event) {}
    @Override
    public void warn(Enum<?> event, Throwable cause) {}

    @Override
    public void error(String message) {}
    @Override
    public void error(String message, Throwable cause) {}
    @Override
    public void error(Enum<?> event) {}
    @Override
    public void error(Enum<?> event, Throwable cause) {}
}
