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

import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.LinkedHashMap;

/**
 * List all the available commands
 */
public final class HedwigCommands {

    static final String[] EMPTY_ARRAY = new String[0];

    //
    // List all commands used to play with hedwig
    //

    /* PUB : publish a message to hedwig */
    static final String PUB = "pub";
    static final String PUB_DESC = "Publish a message to a topic in Hedwig";
    static final String[] PUB_USAGE = new String[] {
        "usage: pub {topic} {message}",
        "",
        "  {topic}   : topic name.",
        "              any printable string without spaces.",
        "  {message} : message body.",
        "              remaining arguments are used as message body to publish.",
    };

    /* SUB : subscriber a topic in hedwig for a specified subscriber */
    static final String SUB = "sub";
    static final String SUB_DESC = "Subscribe a topic for a specified subscriber";
    static final String[] SUB_USAGE = new String[] {
        "usage: sub {topic} {subscriber} [mode]",
        "",
        "  {topic}      : topic name.",
        "                 any printable string without spaces.",
        "  {subscriber} : subscriber id.",
        "                 any printable string without spaces.",
        "  [mode]       : mode to create subscription.",
        "  [receive]    : bool. whether to start delivery to receive messages.",
        "",
        "  available modes: (default value is 1)",
        "    0 = CREATE: create subscription.",
        "                if the subscription is exsited, it will fail.",
        "    1 = ATTACH: attach to exsited subscription.",
        "                if the subscription is not existed, it will faile.",
        "    2 = CREATE_OR_ATTACH:",
        "                attach to subscription, if not existed create one."
    };

    /* CLOSESUB : close the subscription of a subscriber for a topic */
    static final String CLOSESUB = "closesub";
    static final String CLOSESUB_DESC = "Close subscription of a subscriber to a specified topic";
    static final String[] CLOSESUB_USAGE = new String[] {
        "usage: closesub {topic} {subscriber}",
        "",
        "  {topic}      : topic name.",
        "                 any printable string without spaces.",
        "  {subscriber} : subscriber id.",
        "                 any printable string without spaces.",
        "",
        " NOTE: this command just cleanup subscription states on client side.",
        "       You can try UNSUB to clean subscription states on server side.",
    };

    /* UNSUB: unsubscribe of a subscriber to a topic */
    static final String UNSUB = "unsub";
    static final String UNSUB_DESC = "Unsubscribe a topic for a subscriber";
    static final String[] UNSUB_USAGE = new String[] {
        "usage: unsub {topic} {subscriber}",
        "",
        "  {topic}      : topic name.",
        "                 any printable string without spaces.",
        "  {subscriber} : subscriber id.",
        "                 any printable string without spaces.",
        "",
        " NOTE: this command will cleanup subscription states on server side.",
        "       You can try CLOSESUB to just clean subscription states on client side.",
    };

    static final String RMSUB = "rmsub";
    static final String RMSUB_DESC = "Remove subscriptions for topics";
    static final String[] RMSUB_USAGE = new String[] {
        "usage: rmsub {topic_prefix} {start_topic} {end_topic} {subscriber_prefix} {start_sub} {end_sub}",
        "",
        "  {topic_prefix}       : topic prefix.",
        "  {start_topic}        : start topic id.",
        "  {end_topic}          : end topic id.",
        "  {subscriber_prefix}  : subscriber prefix.",
        "  {start_sub}          : start subscriber id.",
        "  {end_sub}            : end subscriber id.",
    };

    /* CONSUME: move consume ptr of a subscription with specified steps */
    static final String CONSUME = "consume";
    static final String CONSUME_DESC = "Move consume ptr of a subscription with sepcified steps";
    static final String[] CONSUME_USAGE = new String[] {
        "usage: consume {topic} {subscriber} {nmsgs}",
        "",
        "  {topic}      : topic name.",
        "                 any printable string without spaces.",
        "  {subscriber} : subscriber id.",
        "                 any printable string without spaces.",
        "  {nmsgs}      : how many messages to move consume ptr.",
        "",
        "  Example:",
        "  suppose, from zk we know subscriber B consumed topic T to message 10",
        "  [hedwig: (standalone) 1] consume T B 2",
        "  after executed above command, a consume(10+2) request will be sent to hedwig.",
        "",
        "  NOTE:",
        "  since Hedwig updates subscription consume ptr lazily, so you need to know that",
        "    1) the consumption ptr read from zookeeper may be stable; ",
        "    2) after sent the consume request, hedwig may just move ptr in its memory and lazily update it to zookeeper. you may not see the ptr changed when DESCRIBE the topic.",
    };

    /* CONSUMETO: move consume ptr of a subscription to a specified pos */
    static final String CONSUMETO = "consumeto";
    static final String CONSUMETO_DESC = "Move consume ptr of a subscription to a specified message id";
    static final String[] CONSUMETO_USAGE = new String[] {
        "usage: consumeto {topic} {subscriber} {msg_id}",
        "",
        "  {topic}      : topic name.",
        "                 any printable string without spaces.",
        "  {subscriber} : subscriber id.",
        "                 any printable string without spaces.",
        "  {msg_id}     : message id that consume ptr will be moved to.",
        "                 if the message id is less than current consume ptr,",
        "                 hedwig will do nothing.",
        "",
        "  Example:",
        "  suppose, from zk we know subscriber B consumed topic T to message 10",
        "  [hedwig: (standalone) 1] consumeto T B 12",
        "  after executed above command, a consume(12) request will be sent to hedwig.",
        "",
        "  NOTE:",
        "  since Hedwig updates subscription consume ptr lazily, so you need to know that",
        "    1) the consumption ptr read from zookeeper may be stable; ",
        "    2) after sent the consume request, hedwig may just move ptr in its memory and lazily update it to zookeeper. you may not see the ptr changed when DESCRIBE the topic.",
    };

    /* PUBSUB: a healthy checking command to ensure cluster is running */
    static final String PUBSUB = "pubsub";
    static final String PUBSUB_DESC = "A healthy checking command to ensure hedwig is in running state";
    static final String[] PUBSUB_USAGE = new String[] {
        "usage: pubsub {topic} {subscriber} {timeout_secs} {message}",
        "",
        "  {topic}        : topic name.",
        "                   any printable string without spaces.",
        "  {subscriber}   : subscriber id.",
        "                   any printable string without spaces.",
        "  {timeout_secs} : how long will the subscriber wait for published message.",
        "  {message}      : message body.",
        "                   remaining arguments are used as message body to publish.",
        "",
        "  Example:",
        "  [hedwig: (standalone) 1] pubsub TOPIC SUBID 10 TEST_MESSAGS",
        "",
        "  1) hw will subscribe topic TOPIC as subscriber SUBID;",
        "  2) subscriber SUBID will wait a message until 10 seconds;",
        "  3) hw publishes TEST_MESSAGES to topic TOPIC;",
        "  4) if subscriber recevied message in 10 secs, it checked that whether the message is published message.",
        "     if true, it will return SUCCESS, otherwise return FAILED.",
    };

    //
    // List all commands used to admin hedwig
    //

    /* SHOW: list all available hub servers or topics */
    static final String SHOW = "show";
    static final String SHOW_DESC = "list all available hub servers or topics";
    static final String[] SHOW_USAGE = new String[] {
        "usage: show [topics | hubs]",
        "",
        "  show topics :",
        "    listing all available topics in hedwig.",
        "",
        "  show hubs :",
        "    listing all available hubs in hedwig.",
        "",
        "  NOTES:",
        "  'show topics' will not works when there are millions of topics in hedwig, since we have packetLen limitation fetching data from zookeeper.",
    };

    static final String SHOW_TOPICS = "topics";
    static final String SHOW_HUBS   = "hubs";

    /* DESCRIBE: show the metadata of a topic */
    static final String DESCRIBE = "describe";
    static final String DESCRIBE_DESC = "show metadata of a topic, including topic owner, persistence info, subscriptions info";
    static final String[] DESCRIBE_USAGE = new String[] {
        "usage: describe topic {topic}",
        "",
        "  {topic} : topic name.",
        "            any printable string without spaces.",
        "",
        "  Example: describe topic ttttt",
        "",
        "  Output:",
        "  ===== Topic Information : ttttt =====",
        "",
        "  Owner : 98.137.99.27:9875:9876",
        "",
        "  >>> Persistence Info <<<",
        "  Ledger 54729 [ 1 ~ 59 ]",
        "  Ledger 54731 [ 60 ~ 60 ]",
        "  Ledger 54733 [ 61 ~ 61 ]",
        "",
        "  >>> Subscription Info <<<",
        "  Subscriber mysub : consumeSeqId: local:50",
    };

    static final String DESCRIBE_TOPIC = "topic";

    /* READTOPIC: read messages of a specified topic */
    static final String READTOPIC = "readtopic";
    static final String READTOPIC_DESC = "read messages of a specified topic";
    static final String[] READTOPIC_USAGE = new String[] {
        "usage: readtopic {topic} [start_msg_id]",
        "",
        "  {topic}        : topic name.",
        "                   any printable string without spaces.",
        "  [start_msg_id] : message id that start to read from.",
        "",
        "  no start_msg_id provided:",
        "    it will start from least_consumed_message_id + 1.",
        "    least_consume_message_id is computed from all its subscribers.",
        "",
        "  start_msg_id provided:",
        "    it will start from MAX(start_msg_id, least_consumed_message_id).",
        "",
        "  MESSAGE FORMAT:",
        "",
        "  ---------- MSGID=LOCAL(51) ----------",
        "  MsgId:     LOCAL(51)",
        "  SrcRegion: standalone",
        "  Message:",
        "",
        "  hello",
    };

    /* FORMAT: format metadata for Hedwig */
    static final String FORMAT = "format";
    static final String FORMAT_DESC = "format metadata for Hedwig";
    static final String[] FORMAT_USAGE = new String[] {
        "usage: format [-force]",
        "",
        "  [-force] : Format metadata for Hedwig w/o confirmation.",
    };


    //
    // List other useful commands
    //

    /* SET: set whether printing zk watches or not */
    static final String SET = "set";
    static final String SET_DESC = "set whether printing zk watches or not";
    static final String[] SET_USAGE = EMPTY_ARRAY;

    /* HISTORY: list history commands */
    static final String HISTORY = "history";
    static final String HISTORY_DESC = "list history commands";
    static final String[] HISTORY_USAGE = EMPTY_ARRAY;

    /* REDO: redo previous command */
    static final String REDO = "redo";
    static final String REDO_DESC = "redo history command";
    static final String[] REDO_USAGE = new String[] {
        "usage: redo [{cmdno} | !]",
        "",
        "  {cmdno} : history command no.",
        "  !       : last command.",
    };

    /* HELP: print usage information of a specified command */
    static final String HELP = "help";
    static final String HELP_DESC = "print usage information of a specified command";
    static final String[] HELP_USAGE = new String[] {
        "usage: help {command}",
        "",
        "  {command} : command name",
    };

    static final String QUIT = "quit";
    static final String QUIT_DESC = "exit console";
    static final String[] QUIT_USAGE = EMPTY_ARRAY;

    static final String EXIT = "exit";
    static final String EXIT_DESC = QUIT_DESC;
    static final String[] EXIT_USAGE = EMPTY_ARRAY;

    public static enum COMMAND {

        CMD_PUB (PUB, PUB_DESC, PUB_USAGE),
        CMD_SUB (SUB, SUB_DESC, SUB_USAGE),
        CMD_CLOSESUB (CLOSESUB, CLOSESUB_DESC, CLOSESUB_USAGE),
        CMD_UNSUB (UNSUB, UNSUB_DESC, UNSUB_USAGE),
        CMD_RMSUB (RMSUB, RMSUB_DESC, RMSUB_USAGE),
        CMD_CONSUME (CONSUME, CONSUME_DESC, CONSUME_USAGE),
        CMD_CONSUMETO (CONSUMETO, CONSUMETO_DESC, CONSUMETO_USAGE),
        CMD_PUBSUB (PUBSUB, PUBSUB_DESC, PUBSUB_USAGE),
        CMD_SHOW (SHOW, SHOW_DESC, SHOW_USAGE),
        CMD_DESCRIBE (DESCRIBE, DESCRIBE_DESC, DESCRIBE_USAGE),
        CMD_READTOPIC (READTOPIC, READTOPIC_DESC, READTOPIC_USAGE),
        CMD_FORMAT (FORMAT, FORMAT_DESC, FORMAT_USAGE),
        CMD_SET (SET, SET_DESC, SET_USAGE),
        CMD_HISTORY (HISTORY, HISTORY_DESC, HISTORY_USAGE),
        CMD_REDO (REDO, REDO_DESC, REDO_USAGE),
        CMD_HELP (HELP, HELP_DESC, HELP_USAGE),
        CMD_QUIT (QUIT, QUIT_DESC, QUIT_USAGE),
        CMD_EXIT (EXIT, EXIT_DESC, EXIT_USAGE),
        // sub commands
        CMD_SHOW_TOPICS (SHOW_TOPICS, "", EMPTY_ARRAY),
        CMD_SHOW_HUBS (SHOW_HUBS, "", EMPTY_ARRAY),
        CMD_DESCRIBE_TOPIC (DESCRIBE_TOPIC, "", EMPTY_ARRAY);

        COMMAND(String name, String desc, String[] usage) {
            this.name = name;
            this.desc = desc;
            this.usage = usage;
            this.subCmds = new LinkedHashMap<String, COMMAND>();
        }

        public String getName() { return name; }

        public String getDescription() { return desc; }

        public Map<String, COMMAND> getSubCommands() { return subCmds; }

        public void addSubCommand(COMMAND c) {
            this.subCmds.put(c.name, c);
        };

        public void printUsage() {
            System.err.println(name + ": " + desc);
            for(String line : usage) {
                System.err.println(line);
            }
            System.err.println();
        }

        protected String name;
        protected String desc;
        protected String[] usage;
        protected Map<String, COMMAND> subCmds;
    }

    static Map<String, COMMAND> commands = null;

    private static void addCommand(COMMAND c) {
        commands.put(c.getName(), c);
    }

    static synchronized void init() {
        if (commands != null) {
            return;
        }
        commands = new LinkedHashMap<String, COMMAND>();

        addCommand(COMMAND.CMD_PUB);
        addCommand(COMMAND.CMD_SUB);
        addCommand(COMMAND.CMD_CLOSESUB);
        addCommand(COMMAND.CMD_UNSUB);
        addCommand(COMMAND.CMD_RMSUB);
        addCommand(COMMAND.CMD_CONSUME);
        addCommand(COMMAND.CMD_CONSUMETO);
        addCommand(COMMAND.CMD_PUBSUB);

        // show
        COMMAND.CMD_SHOW.addSubCommand(COMMAND.CMD_SHOW_TOPICS);
        COMMAND.CMD_SHOW.addSubCommand(COMMAND.CMD_SHOW_HUBS);
        addCommand(COMMAND.CMD_SHOW);

        // describe
        COMMAND.CMD_DESCRIBE.addSubCommand(COMMAND.CMD_DESCRIBE_TOPIC);
        addCommand(COMMAND.CMD_DESCRIBE);

        addCommand(COMMAND.CMD_READTOPIC);
        addCommand(COMMAND.CMD_FORMAT);
        addCommand(COMMAND.CMD_SET);
        addCommand(COMMAND.CMD_HISTORY);
        addCommand(COMMAND.CMD_REDO);
        addCommand(COMMAND.CMD_HELP);
        addCommand(COMMAND.CMD_QUIT);
        addCommand(COMMAND.CMD_EXIT);
    }

    public static Map<String, COMMAND> getHedwigCommands() {
        return commands;
    }

    /**
     * Find candidate commands by the specified token list
     *
     * @param token token list
     *
     * @return list of candidate commands
     */
    public static List<String> findCandidateCommands(String[] tokens) {
        List<String> cmds = new LinkedList<String>();

        Map<String, COMMAND> cmdMap = commands;
        for (int i=0; i<(tokens.length - 1); i++) {
            COMMAND c = cmdMap.get(tokens[i]);
            // no commands
            if (c == null || c.getSubCommands().size() <= 0) {
                return cmds;
            } else {
                cmdMap = c.getSubCommands();
            }
        }
        cmds.addAll(cmdMap.keySet());
        return cmds;
    }
}
