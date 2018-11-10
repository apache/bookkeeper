/*
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

package org.apache.bookkeeper.tools.framework;

/**
 * Command of a cli.
 */
public interface Command<GlobalFlagsT extends CliFlags> {

    /**
     * Returns whether to hide this command from showing in help message.
     *
     * @return true if hide this command from help message.
     */
    default boolean hidden() {
        return false;
    }

    /**
     * Return category of this command belongs to.
     *
     * @return category name
     */
    default String category() {
        return "";
    }

    /**
     * Return command name.
     *
     * @return command name.
     */
    String name();

    /**
     * Return command path in a cli path.
     *
     * <p>This is used for printing usage information.
     *
     * @return command path
     */
    default String path() {
        return name();
    }

    /**
     * Return description name.
     *
     * @return description name.
     */
    String description();

    /**
     * Process the command.
     *
     * @param args command args
     * @return true if successfully apply the args, otherwise false
     */
    Boolean apply(GlobalFlagsT globalFlagsT,
                  String[] args) throws Exception;

    /**
     * Print the help information.
     */
    void usage();
}
