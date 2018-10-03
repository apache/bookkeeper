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

package org.apache.bookkeeper.stream.cli.commands;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.tools.common.BKCommand;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.commons.configuration.CompositeConfiguration;


/**
 * Abstract stream storage related commands.
 */
@Slf4j
abstract class AbstractStreamCommand<CommandFlagsT extends CliFlags> extends BKCommand<CommandFlagsT> {

    protected AbstractStreamCommand(CliSpec<CommandFlagsT> spec) {
        super(spec);
    }

    @Override
    protected boolean acceptServiceUri(ServiceURI serviceURI) {
        return ServiceURI.SERVICE_BK.equals(serviceURI.getServiceName());
    }

    @Override
    protected boolean apply(ServiceURI serviceURI,
                            CompositeConfiguration conf,
                            BKFlags globalFlags,
                            CommandFlagsT cmdFlags) {
        if (serviceURI == null) {
            serviceURI = ServiceURI.DEFAULT_LOCAL_STREAM_STORAGE_SERVICE_URI;
            log.info("Service Uri is not specified. Using default service uri : {}", serviceURI);
        }
        return doApply(serviceURI, conf, globalFlags, cmdFlags);
    }

    protected abstract boolean doApply(ServiceURI serviceURI,
                                       CompositeConfiguration conf,
                                       BKFlags globalFlags,
                                       CommandFlagsT cmdFlags);
}
