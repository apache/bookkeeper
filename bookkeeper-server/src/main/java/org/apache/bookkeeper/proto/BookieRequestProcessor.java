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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.util.MathUtils;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookieRequestProcessor implements RequestProcessor, BookkeeperInternalCallbacks.WriteCallback {

    private final static Logger LOG = LoggerFactory.getLogger(BookieRequestProcessor.class);
    /**
     * The server configuration. We use this for getting the number of add and read
     * worker threads.
     */
    private ServerConfiguration serverCfg;

    /**
     * This is the Bookie instance that is used to handle all read and write requests.
     */
    private Bookie bookie;

    /**
     * The threadpool used to execute all read entry requests issued to this server.
     */
    private final ExecutorService readThreadPool;

    /**
     * The threadpool used to execute all add entry requests issued to this server.
     */
    private final ExecutorService writeThreadPool;

    private final BKStats bkStats = BKStats.getInstance();
    private final boolean statsEnabled;

    public BookieRequestProcessor(ServerConfiguration serverCfg, Bookie bookie) {
        this.serverCfg = serverCfg;
        this.bookie = bookie;
        this.readThreadPool =
            createExecutor(this.serverCfg.getNumReadWorkerThreads(),
                           "BookieWriteThread-" + serverCfg.getBookiePort() + "-%d");
        this.writeThreadPool =
            createExecutor(this.serverCfg.getNumAddWorkerThreads(),
                           "BookieReadThread-" + serverCfg.getBookiePort() + "-%d");
        this.statsEnabled = serverCfg.isStatisticsEnabled();
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
    public void processRequest(BookieProtocol.Request r, Channel c) {
        if (r.getProtocolVersion() < BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION
                        || r.getProtocolVersion() > BookieProtocol.CURRENT_PROTOCOL_VERSION) {
            LOG.error("Invalid protocol version, expected something between "
                            + BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION
                            + " & " + BookieProtocol.CURRENT_PROTOCOL_VERSION
                            + ". got " + r.getProtocolVersion());
            c.write(ResponseBuilder.buildErrorResponse(BookieProtocol.EBADVERSION, r));
            return;
        }

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

    class AddCtx {
        final Channel c;
        final BookieProtocol.AddRequest r;
        final long startTime;

        AddCtx(Channel c, BookieProtocol.AddRequest r) {
            this.c = c;
            this.r = r;

            if (statsEnabled) {
                startTime = MathUtils.now();
            } else {
                startTime = 0;
            }
        }
    }

    private void processAddRequest(final BookieProtocol.Request r, final Channel c) {
        if (null == writeThreadPool) {
            handleAdd(r, c);
        } else {
            writeThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    handleAdd(r, c);
                }
            });
        }
    }

    private void handleAdd(BookieProtocol.Request r, Channel c) {
        assert (r instanceof BookieProtocol.AddRequest);
        BookieProtocol.AddRequest add = (BookieProtocol.AddRequest) r;

        if (bookie.isReadOnly()) {
            LOG.warn("BookieServer is running as readonly mode,"
                            + " so rejecting the request from the client!");
            c.write(ResponseBuilder.buildErrorResponse(BookieProtocol.EREADONLY, add));
            if (statsEnabled) {
                bkStats.getOpStats(BKStats.STATS_ADD).incrementFailedOps();
            }
            return;
        }

        int rc = BookieProtocol.EOK;
        try {
            if (add.isRecoveryAdd()) {
                bookie.recoveryAddEntry(add.getDataAsByteBuffer(), this, new AddCtx(c, add),
                                add.getMasterKey());
            } else {
                bookie.addEntry(add.getDataAsByteBuffer(),
                                this, new AddCtx(c, add), add.getMasterKey());
            }
        } catch (IOException e) {
            LOG.error("Error writing " + add, e);
            rc = BookieProtocol.EIO;
        } catch (BookieException.LedgerFencedException lfe) {
            LOG.error("Attempt to write to fenced ledger", lfe);
            rc = BookieProtocol.EFENCED;
        } catch (BookieException e) {
            LOG.error("Unauthorized access to ledger " + add.getLedgerId(), e);
            rc = BookieProtocol.EUA;
        }
        if (rc != BookieProtocol.EOK) {
            c.write(ResponseBuilder.buildErrorResponse(rc, add));
            if (statsEnabled) {
                bkStats.getOpStats(BKStats.STATS_ADD).incrementFailedOps();
            }
        }
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId,
                    InetSocketAddress addr, Object ctx) {
        assert (ctx instanceof AddCtx);
        AddCtx addctx = (AddCtx) ctx;
        addctx.c.write(ResponseBuilder.buildAddResponse(addctx.r));

        if (statsEnabled) {
            // compute the latency
            if (0 == rc) {
                // for add operations, we compute latency in writeComplete callbacks.
                long elapsedTime = MathUtils.now() - addctx.startTime;
                bkStats.getOpStats(BKStats.STATS_ADD).updateLatency(elapsedTime);
            } else {
                bkStats.getOpStats(BKStats.STATS_ADD).incrementFailedOps();
            }
        }
    }

    private void processReadRequest(final BookieProtocol.Request r, final Channel c) {
        if (null == readThreadPool) {
            handleRead(r, c);
        } else {
            readThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    handleRead(r, c);
                }
            });
        }
    }

    private void handleRead(BookieProtocol.Request r, Channel c) {
        assert (r instanceof BookieProtocol.ReadRequest);
        BookieProtocol.ReadRequest read = (BookieProtocol.ReadRequest) r;

        LOG.debug("Received new read request: {}", r);
        int errorCode = BookieProtocol.EIO;
        long startTime = 0;
        if (statsEnabled) {
            startTime = MathUtils.now();
        }
        ByteBuffer data = null;
        try {
            Future<Boolean> fenceResult = null;
            if (read.isFencingRequest()) {
                LOG.warn("Ledger " + r.getLedgerId() + " fenced by " + c.getRemoteAddress());

                if (read.hasMasterKey()) {
                    fenceResult = bookie.fenceLedger(read.getLedgerId(), read.getMasterKey());
                } else {
                    LOG.error("Password not provided, Not safe to fence {}", read.getLedgerId());
                    if (statsEnabled) {
                        bkStats.getOpStats(BKStats.STATS_READ).incrementFailedOps();
                    }
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                }
            }
            data = bookie.readEntry(r.getLedgerId(), r.getEntryId());
            LOG.debug("##### Read entry ##### {}", data.remaining());
            if (null != fenceResult) {
                // TODO:
                // currently we don't have readCallback to run in separated read
                // threads. after BOOKKEEPER-429 is complete, we could improve
                // following code to make it not wait here
                //
                // For now, since we only try to wait after read entry. so writing
                // to journal and read entry are executed in different thread
                // it would be fine.
                try {
                    Boolean fenced = fenceResult.get(1000, TimeUnit.MILLISECONDS);
                    if (null == fenced || !fenced) {
                        // if failed to fence, fail the read request to make it retry.
                        errorCode = BookieProtocol.EIO;
                        data = null;
                    } else {
                        errorCode = BookieProtocol.EOK;
                    }
                } catch (InterruptedException ie) {
                    LOG.error("Interrupting fence read entry " + read, ie);
                    errorCode = BookieProtocol.EIO;
                    data = null;
                } catch (ExecutionException ee) {
                    LOG.error("Failed to fence read entry " + read, ee);
                    errorCode = BookieProtocol.EIO;
                    data = null;
                } catch (TimeoutException te) {
                    LOG.error("Timeout to fence read entry " + read, te);
                    errorCode = BookieProtocol.EIO;
                    data = null;
                }
            } else {
                errorCode = BookieProtocol.EOK;
            }
        } catch (Bookie.NoLedgerException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = BookieProtocol.ENOLEDGER;
        } catch (Bookie.NoEntryException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = BookieProtocol.ENOENTRY;
        } catch (IOException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = BookieProtocol.EIO;
        } catch (BookieException e) {
            LOG.error("Unauthorized access to ledger " + read.getLedgerId(), e);
            errorCode = BookieProtocol.EUA;
        }

        LOG.trace("Read entry rc = {} for {}",
                        new Object[] { errorCode, read });
        if (errorCode == BookieProtocol.EOK) {
            assert data != null;

            c.write(ResponseBuilder.buildReadResponse(data, read));
            if (statsEnabled) {
                long elapsedTime = MathUtils.now() - startTime;
                bkStats.getOpStats(BKStats.STATS_READ).updateLatency(elapsedTime);
            }
        } else {
            c.write(ResponseBuilder.buildErrorResponse(errorCode, read));
            if (statsEnabled) {
                bkStats.getOpStats(BKStats.STATS_READ).incrementFailedOps();
            }
        }
    }

}
