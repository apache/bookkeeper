/**
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
package org.apache.bookkeeper.proto;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_REQUEST;

public class BookieRequestProcessor implements RequestProcessor {

    private final static Logger LOG = LoggerFactory.getLogger(BookieRequestProcessor.class);
    /**
     * The server configuration. We use this for getting the number of add and read
     * worker threads.
     */
    private final ServerConfiguration serverCfg;

    /**
     * This is the Bookie instance that is used to handle all read and write requests.
     */
    final Bookie bookie;

    /**
     * The threadpool used to execute all read entry requests issued to this server.
     */
    private final ExecutorService readThreadPool;

    /**
     * The threadpool used to execute all add entry requests issued to this server.
     */
    private final ExecutorService writeThreadPool;

    // Expose Stats
    private final BKStats bkStats = BKStats.getInstance();
    private final boolean statsEnabled;
    final OpStatsLogger addRequestStats;
    final OpStatsLogger addEntryStats;
    final OpStatsLogger readRequestStats;
    final OpStatsLogger readEntryStats;

    public BookieRequestProcessor(ServerConfiguration serverCfg, Bookie bookie,
                                  StatsLogger statsLogger) {
        this.serverCfg = serverCfg;
        this.bookie = bookie;
        this.readThreadPool =
            createExecutor(this.serverCfg.getNumReadWorkerThreads(),
                           "BookieReadThread-" + serverCfg.getBookiePort() + "-%d");
        this.writeThreadPool =
            createExecutor(this.serverCfg.getNumAddWorkerThreads(),
                           "BookieWriteThread-" + serverCfg.getBookiePort() + "-%d");
        // Expose Stats
        this.statsEnabled = serverCfg.isStatisticsEnabled();
        this.addEntryStats = statsLogger.getOpStatsLogger(ADD_ENTRY);
        this.addRequestStats = statsLogger.getOpStatsLogger(ADD_ENTRY_REQUEST);
        this.readEntryStats = statsLogger.getOpStatsLogger(READ_ENTRY);
        this.readRequestStats = statsLogger.getOpStatsLogger(READ_ENTRY_REQUEST);
    }

    @Override
    public void close() {
        shutdownExecutor(writeThreadPool);
        shutdownExecutor(readThreadPool);
    }

    private ExecutorService createExecutor(int numThreads, String nameFormat) {
        if (numThreads <= 0) {
            return null;
        } else {
            return Executors.newFixedThreadPool(numThreads,
                new ThreadFactoryBuilder().setNameFormat(nameFormat).build());
        }
    }

    private void shutdownExecutor(ExecutorService service) {
        if (null != service) {
            service.shutdown();
        }
    }

    @Override
    public void processRequest(Object msg, Channel c) {
        // If we can decode this packet as a Request protobuf packet, process
        // it as a version 3 packet. Else, just use the old protocol.
        if (msg instanceof BookkeeperProtocol.Request) {
            BookkeeperProtocol.Request r = (BookkeeperProtocol.Request) msg;
            BookkeeperProtocol.BKPacketHeader header = r.getHeader();
            switch (header.getOperation()) {
                case ADD_ENTRY:
                    processAddRequestV3(r, c);
                    break;
                case READ_ENTRY:
                    processReadRequestV3(r, c);
                    break;
                default:
                    LOG.info("Unknown operation type {}", header.getOperation());
                    BookkeeperProtocol.Response.Builder response =
                            BookkeeperProtocol.Response.newBuilder().setHeader(r.getHeader())
                            .setStatus(BookkeeperProtocol.StatusCode.EBADREQ);
                    c.write(response.build());
                    if (statsEnabled) {
                        bkStats.getOpStats(BKStats.STATS_UNKNOWN).incrementFailedOps();
                    }
                    break;
            }
        } else {
            BookieProtocol.Request r = (BookieProtocol.Request) msg;
            // process packet
            switch (r.getOpCode()) {
                case BookieProtocol.ADDENTRY:
                    processAddRequest(r, c);
                    break;
                case BookieProtocol.READENTRY:
                    processReadRequest(r, c);
                    break;
                default:
                    LOG.error("Unknown op type {}, sending error", r.getOpCode());
                    c.write(ResponseBuilder.buildErrorResponse(BookieProtocol.EBADREQ, r));
                    if (statsEnabled) {
                        bkStats.getOpStats(BKStats.STATS_UNKNOWN).incrementFailedOps();
                    }
                    break;
            }
        }
    }

    private void processAddRequestV3(final BookkeeperProtocol.Request r, final Channel c) {
        WriteEntryProcessorV3 write = new WriteEntryProcessorV3(r, c, this);
        if (null == writeThreadPool) {
            write.run();
        } else {
            writeThreadPool.submit(write);
        }
    }

    private void processReadRequestV3(final BookkeeperProtocol.Request r, final Channel c) {
        ReadEntryProcessorV3 read = new ReadEntryProcessorV3(r, c, this);
        if (null == readThreadPool) {
            read.run();
        } else {
            readThreadPool.submit(read);
        }
    }

    private void processAddRequest(final BookieProtocol.Request r, final Channel c) {
        WriteEntryProcessor write = new WriteEntryProcessor(r, c, this);
        if (null == writeThreadPool) {
            write.run();
        } else {
            writeThreadPool.submit(write);
        }
    }

    private void processReadRequest(final BookieProtocol.Request r, final Channel c) {
        ReadEntryProcessor read = new ReadEntryProcessor(r, c, this);
        if (null == readThreadPool) {
            read.run();
        } else {
            readThreadPool.submit(read);
        }
    }

}
