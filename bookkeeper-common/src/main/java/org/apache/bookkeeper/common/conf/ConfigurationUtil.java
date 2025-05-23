/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.conf;

import java.util.function.Function;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.BasicBuilderParameters;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class ConfigurationUtil {
    /**
     * Create a new PropertiesConfiguration using the given builder function.
     * The purpose of this method is to configure the list handling to behave in the same way as in
     * commons-configuration 1.x.
     * without duplicating the code in multiple places.
     *
     * @param builderFunction a function that takes a Configurations object and returns a FileBasedConfigurationBuilder
     * @return a new PropertiesConfiguration
     * @throws ConfigurationException if there is an error creating the configuration
     */
    public static PropertiesConfiguration newConfiguration(
            Function<Configurations, FileBasedConfigurationBuilder<PropertiesConfiguration>> builderFunction)
            throws ConfigurationException {
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder = builderFunction.apply(new Configurations());
        // configure list handling to behave in the same way as in commons-configuration 1.x
        builder.configure(new BasicBuilderParameters().setListDelimiterHandler(new DefaultListDelimiterHandler(',')));
        return builder.getConfiguration();
    }
}
