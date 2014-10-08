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
package org.apache.hedwig.client.benchmark;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;

import com.google.protobuf.ByteString;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

public class HedwigBenchmark implements Callable<Void> {
    protected static final Logger logger = LoggerFactory.getLogger(HedwigBenchmark.class);

    static final String TOPIC_PREFIX = "topic";

    private final HedwigClient client;
    private final Publisher publisher;
    private final Subscriber subscriber;
    private final CommandLine cmd;

    public HedwigBenchmark(ClientConfiguration cfg, CommandLine cmd) {
        client = new HedwigClient(cfg);
        publisher = client.getPublisher();
        subscriber = client.getSubscriber();
        this.cmd = cmd;
    }

    static boolean amIResponsibleForTopic(int topicNum, int partitionIndex, int numPartitions) {
        return topicNum % numPartitions == partitionIndex;
    }

    @Override
    public Void call() throws Exception {

        //
        // Parameters.
        //

        // What program to run: pub, sub (subscription benchmark), recv.
        final String mode = cmd.getOptionValue("mode","");

        // Number of requests to make (publishes or subscribes).
        int numTopics = Integer.valueOf(cmd.getOptionValue("nTopics", "50"));
        int numMessages = Integer.valueOf(cmd.getOptionValue("nMsgs", "1000"));
        int numRegions = Integer.valueOf(cmd.getOptionValue("nRegions", "1"));
        int startTopicLabel = Integer.valueOf(cmd.getOptionValue("startTopicLabel", "0"));
        int partitionIndex = Integer.valueOf(cmd.getOptionValue("partitionIndex", "0"));
        int numPartitions = Integer.valueOf(cmd.getOptionValue("nPartitions", "1"));

        int replicaIndex = Integer.valueOf(cmd.getOptionValue("replicaIndex", "0"));

        int rate = Integer.valueOf(cmd.getOptionValue("rate", "0"));
        int nParallel = Integer.valueOf(cmd.getOptionValue("npar", "100"));
        int msgSize = Integer.valueOf(cmd.getOptionValue("msgSize", "1024"));

        // Number of warmup subscriptions to make.
        final int nWarmups = Integer.valueOf(cmd.getOptionValue("nwarmups", "1000"));

        if (mode.equals("sub")) {
            BenchmarkSubscriber benchmarkSub = new BenchmarkSubscriber(numTopics, 0, 1, startTopicLabel, 0, 1,
                    subscriber, ByteString.copyFromUtf8("mySub"));

            benchmarkSub.warmup(nWarmups);
            benchmarkSub.call();

        } else if (mode.equals("recv")) {

            BenchmarkSubscriber benchmarkSub = new BenchmarkSubscriber(numTopics, numMessages, numRegions,
                    startTopicLabel, partitionIndex, numPartitions, subscriber, ByteString.copyFromUtf8("sub-"
                            + replicaIndex));

            benchmarkSub.call();

        } else if (mode.equals("pub")) {
            // Offered load in msgs/second.
            BenchmarkPublisher benchmarkPub = new BenchmarkPublisher(numTopics, numMessages, numRegions,
                    startTopicLabel, partitionIndex, numPartitions, publisher, subscriber, msgSize, nParallel, rate);
            benchmarkPub.warmup(nWarmups);
            benchmarkPub.call();

        } else {
            throw new Exception("unknown mode: " + mode);
        }

        return null;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("mode", true, "sub, recv, or pub");
        options.addOption("nTopics", true, "Number of topics, default 50");
        options.addOption("nMsgs", true, "Number of messages, default 1000");
        options.addOption("nRegions", true, "Number of regsions, default 1");
        options.addOption("startTopicLabel", true,
                          "Prefix of topic labels. Must be numeric. Default 0");
        options.addOption("partitionIndex", true, "If partitioning, the partition index for this client");
        options.addOption("nPartitions", true, "Number of partitions, default 1");
        options.addOption("replicaIndex", true, "default 0");
        options.addOption("rate", true, "default 0");
        options.addOption("npar", true, "default 100");
        options.addOption("msgSize", true, "Size of messages, default 1024");
        options.addOption("nwarmups", true, "Number of warmup messages, default 1000");
        options.addOption("defaultHub", true, "Default hedwig hub to connect to, default localhost:4080");

        CommandLineParser parser = new PosixParser();
        final CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("HedwigBenchmark <options>", options);
            System.exit(-1);
        }

        ClientConfiguration cfg = new ClientConfiguration() {
                public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
                    return new HedwigSocketAddress(cmd.getOptionValue("defaultHub",
                                                                      "localhost:4080"));
                }

                public boolean isSSLEnabled() {
                    return false;
                }
            };

        InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

        HedwigBenchmark app = new HedwigBenchmark(cfg, cmd);
        app.call();
        System.exit(0);
    }

}
