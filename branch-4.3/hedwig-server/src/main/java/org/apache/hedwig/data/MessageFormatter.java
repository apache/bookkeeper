/*
 *
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
 *
 */

package org.apache.hedwig.data;

import java.io.IOException;
import java.util.List;

import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.commons.configuration.Configuration;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.RegionSpecificSeqId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Format a pub sub message into a readable format.
 */
public class MessageFormatter extends EntryFormatter {
    static Logger logger = LoggerFactory.getLogger(MessageFormatter.class);

    static final String MESSAGE_PAYLOAD_FORMATTER_CLASS = "message_payload_formatter_class";

    EntryFormatter dataFormatter = EntryFormatter.STRING_FORMATTER;

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        dataFormatter = EntryFormatter.newEntryFormatter(conf, MESSAGE_PAYLOAD_FORMATTER_CLASS);
    }

    @Override
    public void formatEntry(java.io.InputStream input) {
        Message message;
        try {
            message = Message.parseFrom(input);
        } catch (IOException e) {
            System.out.println("WARN: Unreadable message found\n");
            EntryFormatter.STRING_FORMATTER.formatEntry(input);
            return;
        }
        formatMessage(message);
    }

    @Override
    public void formatEntry(byte[] data) {
        Message message;
        try {
            message = Message.parseFrom(data);
        } catch (IOException e) {
            System.out.println("WARN: Unreadable message found\n");
            EntryFormatter.STRING_FORMATTER.formatEntry(data);
            return;
        }
        formatMessage(message);
    }

    void formatMessage(Message message) {
        // print msg id
        String msgId;
        if (!message.hasMsgId()) {
            msgId = "N/A";
        } else {
            MessageSeqId seqId = message.getMsgId();
            StringBuilder idBuilder = new StringBuilder();
            if (seqId.hasLocalComponent()) {
                idBuilder.append("LOCAL(").append(seqId.getLocalComponent()).append(")");
            } else {
                List<RegionSpecificSeqId> remoteIds = seqId.getRemoteComponentsList();
                int i = 0, numRegions = remoteIds.size();
                idBuilder.append("REMOTE(");
                for (RegionSpecificSeqId rssid : remoteIds) {
                    idBuilder.append(rssid.getRegion().toStringUtf8());
                    idBuilder.append("[");
                    idBuilder.append(rssid.getSeqId());
                    idBuilder.append("]");
                    ++i;
                    if (i < numRegions) {
                        idBuilder.append(",");
                    }
                }
                idBuilder.append(")");
            }
            msgId = idBuilder.toString();
        }
        System.out.println("****** MSGID=" + msgId + " ******");
        System.out.println("MessageId:      " + msgId);
        // print source region
        if (message.hasSrcRegion()) {
            System.out.println("SrcRegion:      " + message.getSrcRegion().toStringUtf8());
        } else {
            System.out.println("SrcRegion:      N/A");
        }
        // print message body
        if (message.hasBody()) {
            System.out.println("Body:");
            dataFormatter.formatEntry(message.getBody().toByteArray());
        } else {
            System.out.println("Body:           N/A");
        }
        System.out.println();
    }
}
