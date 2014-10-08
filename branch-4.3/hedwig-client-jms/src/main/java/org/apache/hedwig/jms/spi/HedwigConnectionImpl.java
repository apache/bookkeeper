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
package org.apache.hedwig.jms.spi;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.jms.ConnectionImpl;
import org.apache.hedwig.jms.MessagingSessionFacade;
import org.apache.hedwig.jms.SessionImpl;

import javax.jms.JMSException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 * Returns the hedwig specific functionality of the Connection - which is tied to this spi impl.
 * Coupled with HedwigMessagingSessionFacade.
 *
 */
public class HedwigConnectionImpl extends ConnectionImpl {

    private ClientConfiguration hedwigClientConfig;

    public HedwigConnectionImpl() throws JMSException {
        super();
        init(getUser(), getPassword());
    }

    public HedwigConnectionImpl(String user, String password) throws JMSException {
        super (user, password);
        init(getUser(), getPassword());
    }

    @Override
    protected SessionImpl createSessionInstance(boolean transacted, int acknowledgeMode,
                                                MessagingSessionFacade.DestinationType type) throws JMSException {
        if (null == type) return new SessionImpl(this, transacted, acknowledgeMode);
        switch (type){
            case QUEUE:
                return new QueueSessionImpl(this, transacted, acknowledgeMode);
            case TOPIC:
                return new TopicSessionImpl(this, transacted, acknowledgeMode);
            default:
                throw new JMSException("Unknown type " + type);
        }
    }

    @Override
    protected void doStart(String user, String password) throws JMSException {
        // noop for now ...
    }

    protected void init(String user, String password) throws JMSException {
        // load to check sanity.
        hedwigClientConfig = loadConfig();

        // TODO: Set configuration options specified by the user of api - user/passwd/etc.
    }

    // copied from earlier code ...
    private ClientConfiguration loadConfig() throws JMSException {
        ClientConfiguration config = new ClientConfiguration();

        // TODO: This is not very extensible and useful ... we need to pick the info from
        // configuration specified by user, NOT only from static files !
        // Also, we need to be able to support multiple configuration in a single client !
        // We need a better solution ....

        try {
            // 1. try to load the client configuration as specified from a
            // system property
            if (System.getProperty(HEDWIG_CLIENT_CONFIG_FILE) != null) {
                File configFile = new File(System.getProperty(HEDWIG_CLIENT_CONFIG_FILE));
                if (!configFile.exists()) {
                    throw new JMSException(
                            "Cannot create connection: cannot find Hedwig client configuration file specified as ["
                                    + System.getProperty(HEDWIG_CLIENT_CONFIG_FILE) + "]");
                }
                config.loadConf(configFile.toURI().toURL());
            } else {
                // 2. try to load a "hedwig-client.cfg" file from the classpath
                config.loadConf(new URL(null, "classpath://hedwig-client.cfg", new URLStreamHandler() {
                    protected URLConnection openConnection(URL u) throws IOException {
                        // rely on the relevant classloader - not system classloader.
                        final URL resourceUrl = HedwigConnectionImpl.this.getClass().getClassLoader().
                            getResource(u.getPath());
                        return resourceUrl.openConnection();
                    }
                }));
            }

        } catch (MalformedURLException e) {
            JMSException je = new JMSException("Cannot load Hedwig client configuration file " + e);
            je.setLinkedException(e);
            throw je;
        } catch (ConfigurationException e) {
            JMSException je = new JMSException("Cannot load Hedwig client configuration " + e);
            je.setLinkedException(e);
            throw je;
        }

        /*
        System.out.println("getConsumedMessagesBufferSize : " + config.getConsumedMessagesBufferSize());
        System.out.println("getDefaultServerHost : " + config.getDefaultServerHost());
        System.out.println("isSSLEnabled : " + config.isSSLEnabled());
        System.out.println("getMaximumMessageSize : " + config.getMaximumMessageSize());
        System.out.println("getMaximumOutstandingMessages : " + config.getMaximumOutstandingMessages());
        System.out.println("getMaximumServerRedirects : "  + config.getMaximumServerRedirects());
        System.out.println("getServerAckResponseTimeout : "  + config.getServerAckResponseTimeout());
        */

        return config;
    }

    public ClientConfiguration getHedwigClientConfig() {
        return hedwigClientConfig;
    }

    @Override
    protected void doStop() {
        // nothing specific to be done.
    }

    @Override
    protected void doClose(){
        // nothing specific to be done.
    }

    @Override
    protected MessagingSessionFacade createMessagingSessionFacade(SessionImpl session) throws JMSException {
        return new HedwigMessagingSessionFacade(this, session);
    }

    @Override
    public TopicSessionImpl createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return (TopicSessionImpl) createSessionImpl(transacted, acknowledgeMode,
            MessagingSessionFacade.DestinationType.TOPIC);
    }

    @Override
    public QueueSessionImpl createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return (QueueSessionImpl) createSessionImpl(transacted, acknowledgeMode,
            MessagingSessionFacade.DestinationType.QUEUE);
    }
}
