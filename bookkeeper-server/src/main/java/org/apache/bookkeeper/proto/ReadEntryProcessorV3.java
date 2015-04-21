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
package org.apache.bookkeeper.proto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.util.MathUtils;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

class ReadEntryProcessorV3 extends PacketProcessorBaseV3 implements Runnable {

    private final static Logger LOG = LoggerFactory.getLogger(ReadEntryProcessorV3.class);

    public ReadEntryProcessorV3(Request request, Channel channel,
                                BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
    }

    private ReadResponse getReadResponse() {
        long startTimeNanos = MathUtils.nowInNano();
        ReadRequest readRequest = request.getReadRequest();
        long ledgerId = readRequest.getLedgerId();
        long entryId = readRequest.getEntryId();

        ReadResponse.Builder readResponse = ReadResponse.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        if (!isVersionCompatible()) {
            readResponse.setStatus(StatusCode.EBADVERSION);
            return readResponse.build();
        }

        LOG.debug("Received new read request: {}", request);
        StatusCode status;
        ByteBuffer entryBody;
        try {
            Future<Boolean> fenceResult = null;
            if (readRequest.hasFlag() && readRequest.getFlag().equals(ReadRequest.Flag.FENCE_LEDGER)) {
                LOG.warn("Ledger fence request received for ledger: {} from address: {}", ledgerId,
                         channel.getRemoteAddress());

                if (readRequest.hasMasterKey()) {
                    byte[] masterKey = readRequest.getMasterKey().toByteArray();
                    fenceResult = requestProcessor.bookie.fenceLedger(ledgerId, masterKey);
                } else {
                    LOG.error("Fence ledger request received without master key for ledger:{} from address: {}",
                              ledgerId, channel.getRemoteAddress());
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                }
            }
            entryBody = requestProcessor.bookie.readEntry(ledgerId, entryId);
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
                        status = StatusCode.EIO;
                    } else {
                        status = StatusCode.EOK;
                        readResponse.setBody(ByteString.copyFrom(entryBody));
                    }
                } catch (InterruptedException ie) {
                    LOG.error("Interrupting fence read entry (lid: {}, eid: {})",
                              new Object[] { ledgerId, entryId, ie });
                    status = StatusCode.EIO;
                } catch (ExecutionException ee) {
                    LOG.error("Failed to fence read entry (lid: {}, eid: {})",
                              new Object[] { ledgerId, entryId, ee });
                    status = StatusCode.EIO;
                } catch (TimeoutException te) {
                    LOG.error("Timeout to fence read entry (lid: {}, eid: {})",
                              new Object[] { ledgerId, entryId, te });
                    status = StatusCode.EIO;
                }
            } else {
                readResponse.setBody(ByteString.copyFrom(entryBody));
                status = StatusCode.EOK;
            }
        } catch (Bookie.NoLedgerException e) {
            status = StatusCode.ENOLEDGER;
            LOG.error("No ledger found while reading entry:{} from ledger: {}", entryId, ledgerId);
        } catch (Bookie.NoEntryException e) {
            status = StatusCode.ENOENTRY;
            if (LOG.isDebugEnabled()) {
                LOG.debug("No entry found while reading entry:{} from ledger:{}", entryId, ledgerId);
            }
        } catch (IOException e) {
            status = StatusCode.EIO;
            LOG.error("IOException while reading entry:{} from ledger:{}", entryId, ledgerId);
        } catch (BookieException e) {
            LOG.error("Unauthorized access to ledger:{} while reading entry:{} in request from address: {}",
                    new Object[]{ledgerId, entryId, channel.getRemoteAddress()});
            status = StatusCode.EUA;
        }

        if (status == StatusCode.EOK) {
            requestProcessor.readEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
        } else {
            requestProcessor.readEntryStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
        }

        // Finally set status and return. The body would have been updated if
        // a read went through.
        readResponse.setStatus(status);
        return readResponse.build();
    }

    @Override
    public void run() {
        ReadResponse readResponse = getReadResponse();
        sendResponse(readResponse);
    }

    private void sendResponse(ReadResponse readResponse) {
        Response.Builder response = Response.newBuilder()
                .setHeader(getHeader())
                .setStatus(readResponse.getStatus())
                .setReadResponse(readResponse);
        sendResponse(response.getStatus(),
                     response.build(),
                     requestProcessor.readRequestStats);
    }
}

