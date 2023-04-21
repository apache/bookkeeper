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

package org.apache.bookkeeper.common.conf;

/**
 * Exception thrown for configuration errors.
 */
public class ConfigException extends Exception {

    private static final long serialVersionUID = -7842276571881795108L;

    /**
     * Construct a config exception with provided error.
     *
     * @param error error message
     */
    public ConfigException(String error) {
        super(error);
    }

    /**
     * Construct a config exception with provided error and reason.
     *
     * @param error error message
     * @param cause error cause
     */
    public ConfigException(String error, Throwable cause) {
        super(error, cause);
    }
}
