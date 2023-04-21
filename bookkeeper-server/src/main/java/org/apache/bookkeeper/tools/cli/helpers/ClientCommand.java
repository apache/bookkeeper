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
package org.apache.bookkeeper.tools.cli.helpers;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.common.BKCommand;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.commons.configuration.CompositeConfiguration;

/**
 * This is a mixin class for commands that needs a bookkeeper client.
 */
@Slf4j
public abstract class ClientCommand<ClientFlagsT extends CliFlags> extends BKCommand<ClientFlagsT> {

    protected ClientCommand(CliSpec<ClientFlagsT> spec) {
        super(spec);
    }

    @Override
    protected boolean apply(ServiceURI serviceURI,
                            CompositeConfiguration conf,
                            BKFlags globalFlags,
                            ClientFlagsT cmdFlags) {
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.loadConf(conf);

        if (null != serviceURI) {
            clientConf.setMetadataServiceUri(serviceURI.getUri().toString());
        }

        return apply(clientConf, cmdFlags);
    }

    public boolean apply(ServerConfiguration conf,
                         ClientFlagsT cmdFlags) {
        ClientConfiguration clientConf = new ClientConfiguration(conf);
        return apply(clientConf, cmdFlags);
    }

    protected boolean apply(ClientConfiguration conf,
                            ClientFlagsT cmdFlags) {
        try (BookKeeper bk = BookKeeper.newBuilder(conf).build()) {
            run(bk, cmdFlags);
            return true;
        } catch (Exception e) {
            log.error("Failed to process command '{}'", name(), e);
            return false;
        }
    }

    protected abstract void run(BookKeeper bk, ClientFlagsT cmdFlags)
        throws Exception;

}
