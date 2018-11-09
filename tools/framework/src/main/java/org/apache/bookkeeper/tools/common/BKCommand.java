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

package org.apache.bookkeeper.tools.common;

import com.google.common.base.Strings;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Private;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.tools.framework.Cli;
import org.apache.bookkeeper.tools.framework.CliCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Base bk command class.
 */
@Slf4j
public abstract class BKCommand<CommandFlagsT extends CliFlags> extends CliCommand<BKFlags, CommandFlagsT> {

    protected BKCommand(CliSpec<CommandFlagsT> spec) {
        super(spec);
    }

    @Override
    public Boolean apply(BKFlags globalFlags, String[] args) {
        CliSpec<CommandFlagsT> newSpec = CliSpec.newBuilder(spec)
            .withRunFunc(cmdFlags -> apply(globalFlags, cmdFlags))
            .build();
        return 0 == Cli.runCli(newSpec, args);
    }

    /**
     * Made this as public for allowing old bookie shell use new cli command.
     * This should be removed once we get rid of the old bookie shell.
     */
    @Private
    public int apply(String commandName, CompositeConfiguration conf, String[] args) {
        CliSpec<CommandFlagsT> newSpec = CliSpec.newBuilder(spec)
            .withName(commandName)
            .withRunFunc(cmdFlags -> apply(null, conf, new BKFlags(), cmdFlags))
            .build();
        return Cli.runCli(newSpec, args);
    }

    protected boolean apply(BKFlags bkFlags, CommandFlagsT cmdFlags) {
        ServiceURI serviceURI = null;

        if (null != bkFlags.serviceUri) {
            serviceURI = ServiceURI.create(bkFlags.serviceUri);
            if (!acceptServiceUri(serviceURI)) {
                log.error("Unresolvable service uri by command '{}' : {}",
                    path(), bkFlags.serviceUri);
                return false;
            }
        }

        CompositeConfiguration conf = new CompositeConfiguration();
        if (!Strings.isNullOrEmpty(bkFlags.configFile)) {
            try {
                URL configFileUrl = Paths.get(bkFlags.configFile).toUri().toURL();
                PropertiesConfiguration loadedConf = new PropertiesConfiguration(configFileUrl);
                conf.addConfiguration(loadedConf);
            } catch (MalformedURLException e) {
                log.error("Could not open configuration file : {}", bkFlags.configFile, e);
                throw new IllegalArgumentException(e);
            } catch (ConfigurationException e) {
                log.error("Malformed configuration file : {}", bkFlags.configFile, e);
                throw new IllegalArgumentException(e);
            }
        }

        return apply(serviceURI, conf, bkFlags, cmdFlags);
    }

    protected boolean acceptServiceUri(ServiceURI serviceURI) {
        return true;
    }

    protected abstract boolean apply(ServiceURI serviceURI,
                                     CompositeConfiguration conf,
                                     BKFlags globalFlags,
                                     CommandFlagsT cmdFlags);

}
