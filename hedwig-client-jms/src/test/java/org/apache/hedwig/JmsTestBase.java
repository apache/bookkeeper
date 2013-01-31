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

package org.apache.hedwig;

import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.HedwigHubTestBase;
import java.io.*;

/**
 * Does any jms specific initializations
 */
public class JmsTestBase extends HedwigHubTestBase {
    protected String generatedConfig;

    public JmsTestBase(){
        super(1);
    }

    public JmsTestBase(String name){
        super(name, 1);
    }

    private void init() {
        // single bookie
        this.numBookies = 1;
        // is this required ?
        // disable ssl
        this.sslEnabled = false;
        // Not sure why it works only in standalone mode - something for hedwig folks to debug ?
        this.standalone = true;
        // this.standalone = false;
        // required ?
        // this.readDelay = 1L;
    }

    @Override
    protected void setUp() throws Exception {
        init();
        super.setUp();
        // Now generate HEDWIG_CLIENT_CONFIG_FILE and set the right host/port to it.
        this.generatedConfig = generateConfig(new HubClientConfiguration()
                                              .getDefaultServerHedwigSocketAddress().getPort());
        System.setProperty(org.apache.hedwig.jms.ConnectionImpl.HEDWIG_CLIENT_CONFIG_FILE, generatedConfig);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        // best case
        if (null != generatedConfig) {
            (new File(generatedConfig)).delete();
            generatedConfig = null;
        }
    }

    // Override only standalone, sslEnabled, server port - the config we rely on.
    protected String generateConfig(int serverPort) throws IOException {
        File configFile = File.createTempFile("jms_", ".conf");
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(configFile), "utf-8"));
        writer.write("# The default Hedwig server host to contact\n"
                     + "default_server_host=" + HOST + ":" + serverPort + "\n");
        writer.write("# Flag indicating if the server should also operate in SSL mode.\nssl_enabled=false\n");
        writer.flush();
        writer.close();
        configFile.deleteOnExit();
        return configFile.getAbsolutePath();
    }

    protected void startHubServers() throws Exception {
        super.startHubServers();
        System.out.println("startHubServers done ... " + serversList);
    }


    protected void stopHubServers() throws Exception {
        super.stopHubServers();
        System.out.println("stopHubServers done ... " + serversList);
    }
}
