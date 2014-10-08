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
package org.apache.hedwig.filter;

import java.io.IOException;
import java.util.List;
import java.util.LinkedList;

import com.google.protobuf.ByteString;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;

/**
 * A filter filters messages in pipeline.
 */
public class PipelineFilter extends LinkedList<ServerMessageFilter>
implements ServerMessageFilter {

    @Override
    public ServerMessageFilter initialize(Configuration conf)
    throws ConfigurationException, IOException {
        for (ServerMessageFilter filter : this) {
            filter.initialize(conf);
        }
        return this;
    }

    @Override
    public void uninitialize() {
        while (!isEmpty()) {
            ServerMessageFilter filter = removeLast();
            filter.uninitialize();
        }
    }

    @Override
    public MessageFilterBase setSubscriptionPreferences(ByteString topic, ByteString subscriberId,
                                                        SubscriptionPreferences preferences) {
        for (ServerMessageFilter filter : this) {
            filter.setSubscriptionPreferences(topic, subscriberId, preferences);
        }
        return this;
    }

    @Override
    public boolean testMessage(Message message) {
        for (ServerMessageFilter filter : this) {
            if (!filter.testMessage(message)) {
                return false;
            }
        }
        return true;
    }

}
