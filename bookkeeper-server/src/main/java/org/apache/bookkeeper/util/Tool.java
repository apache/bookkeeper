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

package org.apache.bookkeeper.util;

import org.apache.commons.configuration.CompositeConfiguration;

/**
 * A tool interface that supports handling of generic command-line options.
 */
public interface Tool {
    /**
     * Exectue the command with given arguments.
     *
     * @param args command specific arguments
     * @return exit code.
     * @throws Exception
     */
    int run(String[] args) throws Exception;

    /**
     * Passe a configuration object to the tool.
     *
     * @param conf configuration object
     * @throws Exception
     */
    void setConf(CompositeConfiguration conf) throws Exception;
}
