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

import jline.ConsoleReader;
import jline.History;
import jline.Terminal;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.admin.HedwigAdmin;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRange;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRanges;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.topics.HubInfo;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.hedwig.util.SubscriptionListener;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import static org.apache.hedwig.admin.console.HedwigCommands.*;
import static org.apache.hedwig.admin.console.HedwigCommands.COMMAND.*;

/**
 * Console Client to Hedwig
 */
public class HedwigConsole {
    private static final Logger LOG = LoggerFactory.getLogger(HedwigConsole.class);
    // NOTE: now it is fixed passwd in bookkeeper
    static byte[] passwd = "sillysecret".getBytes();

    // history file name
    static final String HW_HISTORY_FILE = ".hw_history";

    static final char[] CONTINUE_OR_QUIT = new char[] { 'Q', 'q', '\n' };

    protected MyCommandOptions cl = new MyCommandOptions();
    protected HashMap<Integer, String> history = new LinkedHashMap<Integer, String>();
    protected int commandCount = 0;
    protected boolean printWatches = true;
    protected Map<String, MyCommand> myCommands;

    protected boolean inConsole = true;
    protected ConsoleReader console = null;

    protected HedwigAdmin admin;
    protected HedwigClient hubClient;
    protected Publisher publisher;
    protected Subscriber subscriber;
    protected ConsoleMessageHandler consoleHandler =
            new ConsoleMessageHandler();
    protected Terminal terminal;

    protected String myRegion;

    interface MyCommand {
        boolean runCmd(String[] args) throws Exception;
    }

    static class HelpCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            boolean printUsage = true;
            if (args.length >= 2) {
                String command = args[1];
                COMMAND c = getHedwigCommands().get(command);
                if (c != null) {
                    c.printUsage();
                    printUsage = false;
                }
            }
            if (printUsage) {
                usage();
            }
            return true;
        }
    }

    class ExitCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            printMessage("Quitting ...");
            hubClient.close();
            admin.close();
            Runtime.getRuntime().exit(0);
            return true;
        }
    }

    class RedoCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            if (args.length < 2) {
                return false;
            }

            int index;
            if ("!".equals(args[1])) {
                index = commandCount - 1;
            } else {
                index = Integer.decode(args[1]);
                if (commandCount <= index) {
                    System.err.println("Command index out of range");
                    return false;
                }
            }
            cl.parseCommand(history.get(index));
            if (cl.getCommand().equals("redo")) {
                System.err.println("No redoing redos");
                return false;
            }
            history.put(commandCount, history.get(index));
            processCmd(cl);
            return true;
        }
        
    }

    class HistoryCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            for (int i=commandCount - 10; i<=commandCount; ++i) {
                if (i < 0) {
                    continue;
                }
                System.out.println(i + " - " + history.get(i));
            }
            return true;
        }
        
    }

    class SetCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            if (args.length < 3 || !"printwatches".equals(args[1])) {
                return false;
            } else if (args.length == 2) {
                System.out.println("printwatches is " + (printWatches ? "on" : "off"));
            } else {
                printWatches = args[2].equals("on");
            }
            return true;
        }
        
    }

    class PubCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            if (args.length < 3) {
                return false;
            }
            ByteString topic = ByteString.copyFromUtf8(args[1]);

            StringBuilder sb = new StringBuilder();
            for (int i=2; i<args.length; i++) {
                sb.append(args[i]);
                if (i != args.length - 1) {
                    sb.append(' ');
                }
            }
            ByteString msgBody = ByteString.copyFromUtf8(sb.toString());
            Message msg = Message.newBuilder().setBody(msgBody).build();
            try {
                publisher.publish(topic, msg);
                System.out.println("PUB DONE");
            } catch (Exception e) {
                System.err.println("PUB FAILED");
                e.printStackTrace();
            }
            return true;
        }
        
    }

    static class ConsoleMessageHandler implements MessageHandler {

        @Override
        public void deliver(ByteString topic, ByteString subscriberId,
                Message msg, Callback<Void> callback, Object context) {
            System.out.println("Received message from topic " + topic.toStringUtf8() + 
                    " for subscriber " + subscriberId.toStringUtf8() + " : "
                    + msg.getBody().toStringUtf8());
            callback.operationFinished(context, null);
        }
        
    }

    static class ConsoleSubscriptionListener implements SubscriptionListener {

        @Override
        public void processEvent(ByteString t, ByteString s, SubscriptionEvent event) {
            System.out.println("Subscription Channel for (topic:" + t.toStringUtf8() + ", subscriber:"
                                + s.toStringUtf8() + ") received event : " + event);
        }
    }

    class SubCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            CreateOrAttach mode;
            boolean receive = true;
            if (args.length < 3) {
                return false;
            } else if (args.length == 3) {
                mode = CreateOrAttach.ATTACH;
                receive = true;
            } else {
                try {
                    mode = CreateOrAttach.valueOf(Integer.parseInt(args[3]));
                } catch (Exception e) {
                    System.err.println("Unknow mode : " + args[3]);
                    return false;
                }
                if (args.length >= 5) {
                    try {
                        receive = Boolean.parseBoolean(args[4]);
                    } catch (Exception e) {
                        receive = false;
                    }
                }
            }
            if (mode == null) {
                System.err.println("Unknow mode : " + args[3]);
                return false;
            }
            ByteString topic = ByteString.copyFromUtf8(args[1]);
            ByteString subId = ByteString.copyFromUtf8(args[2]);
            try {
                SubscriptionOptions options =
                    SubscriptionOptions.newBuilder().setCreateOrAttach(mode)
                                       .setForceAttach(false).build();
                subscriber.subscribe(topic, subId, options);
                if (receive) {
                    subscriber.startDelivery(topic, subId, consoleHandler);
                    System.out.println("SUB DONE AND RECEIVE");
                } else {
                    System.out.println("SUB DONE BUT NOT RECEIVE");
                }
            } catch (Exception e) {
                System.err.println("SUB FAILED");
                e.printStackTrace();
            }
            return true;
        }
    }

    class UnsubCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            if (args.length < 3) {
                return false;
            }
            ByteString topic = ByteString.copyFromUtf8(args[1]);
            ByteString subId = ByteString.copyFromUtf8(args[2]);
            try {
                subscriber.stopDelivery(topic, subId);
                subscriber.unsubscribe(topic, subId);
                System.out.println("UNSUB DONE");
            } catch (Exception e) {
                System.err.println("UNSUB FAILED");
                e.printStackTrace();
            }
            return true;
        }
        
    }

    class RmsubCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            if (args.length < 7) {
                return false;
            }
            String topicPrefix = args[1];
            int startTopic = Integer.parseInt(args[2]);
            int endTopic = Integer.parseInt(args[3]);
            String subPrefix = args[4];
            int startSub = Integer.parseInt(args[5]);
            int endSub = Integer.parseInt(args[6]);
            if (startTopic > endTopic || endSub < startSub) {
                return false;
            }
            for (int i=startTopic; i<=endTopic; i++) {
                ByteString topic = ByteString.copyFromUtf8(topicPrefix + i);
                try {
                    for (int j=startSub; j<=endSub; j++) {
                        ByteString sub = ByteString.copyFromUtf8(subPrefix + j);
                        subscriber.subscribe(topic, sub, CreateOrAttach.CREATE_OR_ATTACH);
                        subscriber.unsubscribe(topic, sub);
                    }
                    System.out.println("RMSUB " + topic.toStringUtf8() + " DONE");
                } catch (Exception e) {
                    System.err.println("RMSUB " + topic.toStringUtf8() + " FAILED");
                    e.printStackTrace();
                }
            }
            return true;
        }

    }
    
    class CloseSubscriptionCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            if (args.length < 3) {
                return false;
            }
            ByteString topic = ByteString.copyFromUtf8(args[1]);
            ByteString sudId = ByteString.copyFromUtf8(args[2]);
            
            try {
                subscriber.stopDelivery(topic, sudId);
                subscriber.closeSubscription(topic, sudId);
            } catch (Exception e) {
                System.err.println("CLOSESUB FAILED");
            }
            return true;
        }
        
    }
    
    class ConsumeToCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            if (args.length < 4) {
                return false;
            }
            ByteString topic = ByteString.copyFromUtf8(args[1]);
            ByteString subId = ByteString.copyFromUtf8(args[2]);
            long msgId = Long.parseLong(args[3]);
            MessageSeqId consumeId = MessageSeqId.newBuilder().setLocalComponent(msgId).build();
            try {
                subscriber.consume(topic, subId, consumeId);
            } catch (Exception e) {
                System.err.println("CONSUMETO FAILED");
            }
            return true;
        }
        
    }
    
    class ConsumeCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            if (args.length < 4) {
                return false;
            }
            long lastConsumedId = 0;
            SubscriptionData subData = admin.getSubscription(ByteString.copyFromUtf8(args[1]),
                                                             ByteString.copyFromUtf8(args[2]));
            if (null == subData) {
                System.err.println("Failed to read subscription for topic: " + args[1]
                                 + " subscriber: " + args[2]);
                return true;
            }
            lastConsumedId = subData.getState().getMsgId().getLocalComponent();
            long numMessagesToConsume = Long.parseLong(args[3]);
            long idToConsumed = lastConsumedId + numMessagesToConsume;
            System.out.println("Try to move subscriber(" + args[2] + ") consume ptr of topic(" + args[1]
                             + ") from " + lastConsumedId + " to " + idToConsumed);
            MessageSeqId consumeId = MessageSeqId.newBuilder().setLocalComponent(idToConsumed).build();
            ByteString topic = ByteString.copyFromUtf8(args[1]);
            ByteString subId = ByteString.copyFromUtf8(args[2]);
            try {
                subscriber.consume(topic, subId, consumeId);
            } catch (Exception e) {
                System.err.println("CONSUME FAILED");
            }
            return true;
        }
        
    }

    class PubSubCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            if (args.length < 5) {
                return false;
            }
            final long startTime = MathUtils.now();

            final ByteString topic = ByteString.copyFromUtf8(args[1]);
            final ByteString subId = ByteString.copyFromUtf8(args[2] + "-" + startTime);
            int timeoutSecs = 60;
            try {
                timeoutSecs = Integer.parseInt(args[3]);
            } catch (NumberFormatException nfe) {
            }

            StringBuilder sb = new StringBuilder();
            for (int i=4; i<args.length; i++) {
                sb.append(args[i]);
                if (i != args.length - 1) {
                    sb.append(' ');
                }
            }
            // append a timestamp tag
            ByteString msgBody = ByteString.copyFromUtf8(sb.toString() + "-" + startTime);
            final Message msg = Message.newBuilder().setBody(msgBody).build();

            boolean subscribed = false;
            boolean success = false;
            final CountDownLatch isDone = new CountDownLatch(1);
            long elapsedTime = 0L;

            System.out.println("Starting PUBSUB test ...");
            try {
                // sub the topic
                subscriber.subscribe(topic, subId, CreateOrAttach.CREATE_OR_ATTACH);
                subscribed = true;

                System.out.println("Sub topic " + topic.toStringUtf8() + ", subscriber id " + subId.toStringUtf8());

                

                // pub topic
                publisher.publish(topic, msg);
                System.out.println("Pub topic " + topic.toStringUtf8() + " : " + msg.getBody().toStringUtf8());

                // ensure subscriber first, publish next, then we start delivery to receive message
                // if start delivery first before publish, isDone may notify before wait
                subscriber.startDelivery(topic, subId, new MessageHandler() {

                    @Override
                    public void deliver(ByteString thisTopic, ByteString subscriberId,
                            Message message, Callback<Void> callback, Object context) {
                        if (thisTopic.equals(topic) && subscriberId.equals(subId) &&
                            msg.getBody().equals(message.getBody())) {
                            System.out.println("Received message : " + message.getBody().toStringUtf8());
                            isDone.countDown();
                        }
                        callback.operationFinished(context, null);
                    }

                });

                // wait for the message
                success = isDone.await(timeoutSecs, TimeUnit.SECONDS);
                elapsedTime = MathUtils.now() - startTime;
            } finally {
                try {
                    if (subscribed) {
                        subscriber.stopDelivery(topic, subId);
                        subscriber.unsubscribe(topic, subId);
                    }
                } finally {
                    if (success) {
                        System.out.println("PUBSUB SUCCESS. TIME: " + elapsedTime + " MS");
                    } else {
                        System.out.println("PUBSUB FAILED. ");
                    }
                    return success;
                }
            }
        }

    }
    
    class ReadTopicCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            if (args.length < 2) {
                return false;
            }
            ReadTopic rt;
            ByteString topic = ByteString.copyFromUtf8(args[1]);
            if (args.length == 2) {
                rt = new ReadTopic(admin, topic, inConsole);
            } else {
                rt = new ReadTopic(admin, topic, Long.parseLong(args[2]), inConsole);
            }
            rt.readTopic();
            return true;
        }
        
    }

    class ShowCmd implements MyCommand {

        static final int MAX_TOPICS_PER_SHOW = 100;

        @Override
        public boolean runCmd(String[] args) throws Exception {
            if (args.length < 2) {
                return false;
            }
            String errorMsg = null;
            try {
                if (HedwigCommands.SHOW_HUBS.equals(args[1])) {
                    errorMsg = "Unable to fetch the list of hub servers";
                    showHubs();
                } else if (HedwigCommands.SHOW_TOPICS.equals(args[1])) {
                    errorMsg = "Unable to fetch the list of topics";
                    showTopics();
                } else {
                    System.err.println("ERROR: Unknown show command '" + args[1] + "'");
                    return false;
                }
            } catch (Exception e) {
                if (null != errorMsg) {
                    System.err.println(errorMsg);
                }
                e.printStackTrace();
            }
            return true;
        }

        protected void showHubs() throws Exception {
            Map<HedwigSocketAddress, HedwigAdmin.HubStats> hubs = admin.getAvailableHubs();
            System.out.println("Available Hub Servers:");
            for (Map.Entry<HedwigSocketAddress, HedwigAdmin.HubStats> entry : hubs.entrySet()) {
                System.out.println("\t" + entry.getKey() + " :\t" + entry.getValue());
            }
        }

        protected void showTopics() throws Exception {
            List<String> topics = new ArrayList<String>();
            Iterator<ByteString> iter = admin.getTopics();

            System.out.println("Topic List:");
            boolean stop = false;
            while (iter.hasNext()) {
                if (topics.size() >= MAX_TOPICS_PER_SHOW) {
                    System.out.println(topics);
                    topics.clear();
                    stop = !continueOrQuit();
                    if (stop) {
                        break;
                    }
                }
                ByteString t = iter.next();
                topics.add(t.toStringUtf8());
            }
            if (!stop) {
                System.out.println(topics);
            }
        }

        
        
    }

    class DescribeCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            if (args.length < 3) {
                return false;
            }
            if (HedwigCommands.DESCRIBE_TOPIC.equals(args[1])) {
                return describeTopic(args[2]);
            } else {
                return false;
            }
        }

        protected boolean describeTopic(String topic) throws Exception {
            ByteString btopic = ByteString.copyFromUtf8(topic);
            HubInfo owner = admin.getTopicOwner(btopic);
            List<LedgerRange> ranges = admin.getTopicLedgers(btopic);
            Map<ByteString, SubscriptionData> states = admin.getTopicSubscriptions(btopic);

            System.out.println("===== Topic Information : " + topic + " =====");
            System.out.println();
            System.out.println("Owner : " + (owner == null ? "NULL" :
                               owner.toString().trim().replaceAll("\n", ", ")));
            System.out.println();

            // print ledgers
            printTopicLedgers(ranges);
            // print subscriptions
            printTopicSubscriptions(states);

            return true;
        }

        private void printTopicLedgers(List<LedgerRange> ranges) {
            System.out.println(">>> Persistence Info <<<");
            if (null == ranges) {
                System.out.println("N/A");
                return;
            }
            if (ranges.isEmpty()) {
                System.out.println("No Ledger used.");
                return;
            }
            for (LedgerRange range : ranges) {
                System.out.println("Ledger " + range.getLedgerId() + " [ "
                                   + range.getStartSeqIdIncluded() + " ~ "
                                   + range.getEndSeqIdIncluded().getLocalComponent() + " ]");
            }
            System.out.println();
        }

        private void printTopicSubscriptions(Map<ByteString, SubscriptionData> states) {
            System.out.println(">>> Subscription Info <<<");
            if (0 == states.size()) {
                System.out.println("No subscriber.");
                return;
            }
            for (Map.Entry<ByteString, SubscriptionData> entry : states.entrySet()) {
                System.out.println("Subscriber " + entry.getKey().toStringUtf8() + " : "
                                 + SubscriptionStateUtils.toString(entry.getValue()));
            }
            System.out.println();
        }

    }

    class FormatCmd implements MyCommand {

        @Override
        public boolean runCmd(String[] args) throws Exception {
            boolean force = false;
            if (args.length >= 2 && "-force".equals(args[1])) {
                force = true;
            }
            boolean doFormat = true;
            System.out.println("You ask to format hedwig metadata stored in "
                               + admin.getMetadataManagerFactory().getClass().getName() + ".");
            if (!force) {
                doFormat = continueOrQuit();
            }
            if (doFormat) {
                admin.format();
                System.out.println("Formatted hedwig metadata successfully.");
            } else {
                System.out.println("Given up formatting hedwig metadata.");
            }
            return true;
        }

    }

    protected Map<String, MyCommand> buildMyCommands() {
        Map<String, MyCommand> cmds =
                new HashMap<String, MyCommand>();

        ExitCmd exitCmd = new ExitCmd();
        cmds.put(EXIT, exitCmd);
        cmds.put(QUIT, exitCmd);
        cmds.put(HELP, new HelpCmd());
        cmds.put(HISTORY, new HistoryCmd());
        cmds.put(REDO, new RedoCmd());
        cmds.put(SET, new SetCmd());
        cmds.put(PUB, new PubCmd());
        cmds.put(SUB, new SubCmd());
        cmds.put(PUBSUB, new PubSubCmd());
        cmds.put(CLOSESUB, new CloseSubscriptionCmd());
        cmds.put(UNSUB, new UnsubCmd());
        cmds.put(RMSUB, new RmsubCmd());
        cmds.put(CONSUME, new ConsumeCmd());
        cmds.put(CONSUMETO, new ConsumeToCmd());
        cmds.put(SHOW, new ShowCmd());
        cmds.put(DESCRIBE, new DescribeCmd());
        cmds.put(READTOPIC, new ReadTopicCmd());
        cmds.put(FORMAT, new FormatCmd());

        return cmds;
    }

    static void usage() {
        System.err.println("HedwigConsole [options] [command] [args]");
        System.err.println();
        System.err.println("Avaiable commands:");
        for (String cmd : getHedwigCommands().keySet()) {
            System.err.println("\t" + cmd);
        }
        System.err.println();
    }

    /**
     * A storage class for both command line options and shell commands.
     */
    static private class MyCommandOptions {

        private Map<String,String> options = new HashMap<String,String>();
        private List<String> cmdArgs = null;
        private String command = null;

        public MyCommandOptions() {
        }

        public String getOption(String opt) {
            return options.get(opt);
        }

        public String getCommand( ) {
            return command;
        }

        public String getCmdArgument( int index ) {
            return cmdArgs.get(index);
        }

        public int getNumArguments( ) {
            return cmdArgs.size();
        }

        public String[] getArgArray() {
            return cmdArgs.toArray(new String[0]);
        }

        /**
         * Parses a command line that may contain one or more flags
         * before an optional command string
         * @param args command line arguments
         * @return true if parsing succeeded, false otherwise.
         */
        public boolean parseOptions(String[] args) {
            List<String> argList = Arrays.asList(args);
            Iterator<String> it = argList.iterator();

            while (it.hasNext()) {
                String opt = it.next();
                if (!opt.startsWith("-")) {
                    command = opt;
                    cmdArgs = new ArrayList<String>( );
                    cmdArgs.add( command );
                    while (it.hasNext()) {
                        cmdArgs.add(it.next());
                    }
                    return true;
                } else {
                    try {
                        options.put(opt.substring(1), it.next());
                    } catch (NoSuchElementException e) {
                        System.err.println("Error: no argument found for option "
                                + opt);
                        return false;
                    }
                }
            }
            return true;
        }

        /**
         * Breaks a string into command + arguments.
         * @param cmdstring string of form "cmd arg1 arg2..etc"
         * @return true if parsing succeeded.
         */
        public boolean parseCommand( String cmdstring ) {
            String[] args = cmdstring.split(" ");
            if (args.length == 0){
                return false;
            }
            command = args[0];
            cmdArgs = Arrays.asList(args);
            return true;
        }
    }

    private class MyWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (getPrintWatches()) {
                printMessage("WATCHER::");
                printMessage(event.toString());
            }
        }
    }

    public void printMessage(String msg) {
        if (inConsole) {
            System.out.println("\n"+msg);
        }
    }

    /**
     * Hedwig Console
     *
     * @param args arguments
     * @throws IOException
     * @throws InterruptedException 
     */
    public HedwigConsole(String[] args) throws IOException, InterruptedException {
        // Setup Terminal
        terminal = Terminal.setupTerminal();
        HedwigCommands.init();
        cl.parseOptions(args);

        if (cl.getCommand() == null) {
            inConsole = true;
        } else {
            inConsole = false;
        }

        org.apache.bookkeeper.conf.ClientConfiguration bkClientConf =
            new org.apache.bookkeeper.conf.ClientConfiguration();
        ServerConfiguration hubServerConf = new ServerConfiguration();
        String serverCfgFile = cl.getOption("server-cfg");
        if (serverCfgFile != null) {
            try {
                hubServerConf.loadConf(new File(serverCfgFile).toURI().toURL());
            } catch (ConfigurationException e) {
                throw new IOException(e);
            }
            try {
                bkClientConf.loadConf(new File(serverCfgFile).toURI().toURL());
            } catch (ConfigurationException e) {
                throw new IOException(e);
            }
        }

        ClientConfiguration hubClientCfg = new ClientConfiguration();
        String clientCfgFile = cl.getOption("client-cfg");
        if (clientCfgFile != null) {
            try {
                hubClientCfg.loadConf(new File(clientCfgFile).toURI().toURL());
            } catch (ConfigurationException e) {
                throw new IOException(e);
            }
        }

        printMessage("Connecting to zookeeper/bookkeeper using HedwigAdmin");
        try {
            admin = new HedwigAdmin(bkClientConf, hubServerConf);
            admin.getZkHandle().register(new MyWatcher());
        } catch (Exception e) {
            throw new IOException(e);
        }
        
        printMessage("Connecting to default hub server " + hubClientCfg.getDefaultServerHost());
        hubClient = new HedwigClient(hubClientCfg);
        publisher = hubClient.getPublisher();
        subscriber = hubClient.getSubscriber();
        subscriber.addSubscriptionListener(new ConsoleSubscriptionListener());
        
        // other parameters
        myRegion = hubServerConf.getMyRegion();
    }

    public boolean getPrintWatches() {
        return printWatches;
    }

    protected String getPrompt() {
        StringBuilder sb = new StringBuilder();
        sb.append("[hedwig: (").append(myRegion).append(") ").append(commandCount).append("] ");
        return sb.toString();
    }

    protected boolean continueOrQuit() throws IOException {
        System.out.println("Press <Return> to continue, or Q to cancel ...");
        int ch;
        if (null != console) {
            ch = console.readCharacter(CONTINUE_OR_QUIT);
        } else {
            do {
                ch = terminal.readCharacter(System.in);
            } while (ch != 'q' && ch != 'Q' && ch != '\n');
        }
        if (ch == 'q' ||
            ch == 'Q') {
            return false;
        }
        return true;
    }

    protected void addToHistory(int i, String cmd) {
        history.put(i, cmd);
    }

    public void executeLine(String line) {
        if (!line.equals("")) {
            cl.parseCommand(line);
            addToHistory(commandCount, line);
            processCmd(cl);
            commandCount++;
        }
    }

    protected boolean processCmd(MyCommandOptions co) {
        String[] args = co.getArgArray();
        String cmd = co.getCommand();
        if (args.length < 1) {
            usage();
            return false;
        }
        if (!getHedwigCommands().containsKey(cmd)) {
            usage();
            return false;
        }

        LOG.debug("Processing {}", cmd);

        MyCommand myCommand = myCommands.get(cmd);
        if (myCommand == null) {
            System.err.println("No Command Processor found for command " + cmd);
            usage();
            return false;
        }

        long startTime = MathUtils.now();
        boolean success = false;
        try {
            success = myCommand.runCmd(args);
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        }
        long elapsedTime = MathUtils.now() - startTime;
        if (inConsole) {
            if (success) {
                System.out.println("Finished " + ((double)elapsedTime / 1000) + " s.");
            } else {
                COMMAND c = getHedwigCommands().get(cmd);
                if (c != null) {
                    c.printUsage();
                }
            }
        }
        return success;
    }

    @SuppressWarnings("unchecked")
    void run() throws IOException {
        inConsole = true;
        myCommands = buildMyCommands();
        if (cl.getCommand() == null) {
            System.out.println("Welcome to Hedwig!");
            System.out.println("JLine support is enabled");

            console = new ConsoleReader();
            JLineHedwigCompletor completor = new JLineHedwigCompletor(admin);
            console.addCompletor(completor);

            // load history file
            History history = new History();
            File file = new File(System.getProperty("hw.history",
                                 new File(System.getProperty("user.home"), HW_HISTORY_FILE).toString()));
            if (LOG.isDebugEnabled()) {
                LOG.debug("History file is " + file.toString());
            }
            history.setHistoryFile(file);
            // set history to console reader
            console.setHistory(history);
            // load history from history file
            history.moveToFirstEntry();

            while (history.next()) {
                String entry = history.current();
                if (!entry.equals("")) {
                    addToHistory(commandCount, entry);
                }
                commandCount++;
            }
            System.out.println("JLine history support is enabled");

            String line;
            while ((line = console.readLine(getPrompt())) != null) {
                executeLine(line);
                history.addToHistory(line);
            }
        }

        inConsole = false;
        processCmd(cl);
        try {
            myCommands.get(EXIT).runCmd(new String[0]);
        } catch (Exception e) {
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        HedwigConsole console = new HedwigConsole(args);
        console.run();
    }
}
