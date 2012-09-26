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
package org.apache.hedwig.client.netty;

import org.jboss.netty.channel.Channel;
import org.apache.hedwig.client.data.PubSubData;

/**
 * A wrapper interface over netty {@link Channel} to submit hedwig's
 * {@link PubSubData} requests.
 */
public interface HChannel {

    /**
     * Submit a pub/sub request.
     *
     * @param op
     *          Pub/Sub Request.
     */
    public void submitOp(PubSubData op);

    /**
     * @return underlying netty channel
     */
    public Channel getChannel();

    /**
     * Close the channel without waiting.
     */
    public void close();

    /**
     * Close the channel
     *
     * @param wait
     *          Whether wait until the channel is closed.
     */
    public void close(boolean wait);
}
