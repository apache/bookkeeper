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

package org.apache.hedwig.admin.console;

import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.hedwig.admin.HedwigAdmin;

import com.google.protobuf.ByteString;

import jline.Completor;

import static org.apache.hedwig.admin.console.HedwigCommands.*;

/**
 * A jline completor for hedwig console
 */
public class JLineHedwigCompletor implements Completor {
    // for topic completion
    static final int MAX_TOPICS_TO_SEARCH = 1000;

    private HedwigAdmin admin;

    public JLineHedwigCompletor(HedwigAdmin admin) {
        this.admin = admin;
    }

    @Override
    public int complete(String buffer, int cursor, List candidates) {
        // Guarantee that the final token is the one we're expanding
        buffer = buffer.substring(0,cursor);
        String[] tokens = buffer.split(" ");
        if (buffer.endsWith(" ")) {
            String[] newTokens = new String[tokens.length + 1];
            System.arraycopy(tokens, 0, newTokens, 0, tokens.length);
            newTokens[newTokens.length - 1] = "";
            tokens = newTokens;
        }
        
        if (tokens.length > 2 &&
            DESCRIBE.equalsIgnoreCase(tokens[0]) &&
            DESCRIBE_TOPIC.equalsIgnoreCase(tokens[1])) {
            return completeTopic(buffer, tokens[2], candidates);
        } else if (tokens.length > 1 &&
                   (SUB.equalsIgnoreCase(tokens[0]) ||
                    PUB.equalsIgnoreCase(tokens[0]) ||
                    CLOSESUB.equalsIgnoreCase(tokens[0]) ||
                    CONSUME.equalsIgnoreCase(tokens[0]) ||
                    CONSUMETO.equalsIgnoreCase(tokens[0]) ||
                    READTOPIC.equalsIgnoreCase(tokens[0]))) {
            return completeTopic(buffer, tokens[1], candidates);
        }
        List cmds = HedwigCommands.findCandidateCommands(tokens);
        return completeCommand(buffer, tokens[tokens.length - 1], cmds, candidates);
    }

    @SuppressWarnings("unchecked")
    private int completeCommand(String buffer, String token,
            List commands, List candidates) {
        for (Object cmdo : commands) {
            assert (cmdo instanceof String);
            if (((String)cmdo).startsWith(token)) {
                candidates.add(cmdo);
            }
        }
        return buffer.lastIndexOf(" ") + 1;
    }

    @SuppressWarnings("unchecked")
    private int completeTopic(String buffer, String token, List candidates) {
        try {
            Iterator<ByteString> children = admin.getTopics();
            int i = 0;
            while (children.hasNext() && i <= MAX_TOPICS_TO_SEARCH) {
                String child = children.next().toStringUtf8();
                if (child.startsWith(token)) {
                    candidates.add(child);
                }
                ++i;
            }
        } catch (Exception e) {
            return buffer.length();
        }
        return candidates.size() == 0 ? buffer.length() : buffer.lastIndexOf(" ") + 1;
    }
}
