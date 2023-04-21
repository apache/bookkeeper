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
package org.apache.bookkeeper.conf;

import java.util.Objects;
import org.apache.commons.configuration.ConfigurationException;

/**
 * Wraps a {@link org.apache.commons.configuration.ConfigurationException} with an unchecked exception.
 *
 * @since 4.7.0
 */
public class UncheckedConfigurationException extends RuntimeException {

    /**
     * Constructs an instance of this class.
     *
     * @param   message
     *          the detail message, can be null
     * @param   cause
     *          the {@code ConfigurationException}
     *
     * @throws  NullPointerException
     *          if the cause is {@code null}
     */
    public UncheckedConfigurationException(String message, ConfigurationException cause) {
        super(message, Objects.requireNonNull(cause));
    }

    /**
     * Constructs an instance of this class.
     *
     * @param   cause
     *          the {@code IOException}
     *
     * @throws  NullPointerException
     *          if the cause is {@code null}
     */
    public UncheckedConfigurationException(ConfigurationException cause) {
        super(Objects.requireNonNull(cause));
    }

    /**
     * Returns the cause of this exception.
     *
     * @return  the {@code IOException} which is the cause of this exception.
     */
    @Override
    public ConfigurationException getCause() {
        return (ConfigurationException) super.getCause();
    }

}
