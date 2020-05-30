/*
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

package org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.dlog;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.util.Utils;

/**
 * DistributedLog Output Stream.
 */
@Slf4j
class DLOutputStream extends OutputStream {

    private static final byte[] CONTROL_RECORD_CONTENT = "control".getBytes(UTF_8);

    private final DistributedLogManager dlm;
    private final AsyncLogWriter writer;

    // positions
    private final long[] syncPos = new long[1];
    private long writePos = 0L;

    // state
    private static final AtomicReferenceFieldUpdater<DLOutputStream, Throwable> exceptionUpdater =
        AtomicReferenceFieldUpdater.newUpdater(DLOutputStream.class, Throwable.class, "exception");
    private volatile Throwable exception = null;

    DLOutputStream(DistributedLogManager dlm,
                   AsyncLogWriter writer) {
        this.dlm = dlm;
        this.writer = writer;
        this.writePos = writer.getLastTxId() < 0L ? 0L : writer.getLastTxId();
        this.syncPos[0] = writePos;
    }

    public synchronized long position() {
        return syncPos[0];
    }

    @Override
    public void write(int b) throws IOException {
        byte[] data = new byte[] { (byte) b };
        write(data);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(Unpooled.wrappedBuffer(b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        write(Unpooled.wrappedBuffer(b, off, len));
    }

    private synchronized void write(ByteBuf buf) throws IOException {
        Throwable cause = exceptionUpdater.get(this);
        if (null != cause) {
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else {
                throw new UnexpectedException("Encountered unknown issue", cause);
            }
        }

        writePos += buf.readableBytes();
        LogRecord record = new LogRecord(writePos, buf);
        writer.write(record).whenComplete(new FutureEventListener<DLSN>() {
            @Override
            public void onSuccess(DLSN value) {
                synchronized (syncPos) {
                    syncPos[0] = record.getTransactionId();
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                exceptionUpdater.compareAndSet(DLOutputStream.this, null, cause);
            }
        });
    }

    private CompletableFuture<DLSN> writeControlRecord() {
        LogRecord record;
        synchronized (this) {
            record = new LogRecord(writePos, Unpooled.wrappedBuffer(CONTROL_RECORD_CONTENT));
            record.setControl();
        }
        return writer.write(record);
    }

    @Override
    public void flush() throws IOException {
        try {
            FutureUtils.result(writeControlRecord());
        } catch (IOException ioe) {
            throw ioe;
        } catch (Exception e) {
            log.error("Unexpected exception in DLOutputStream", e);
            throw new UnexpectedException("unexpected exception in DLOutputStream#flush()", e);
        }
    }

    @Override
    public void close() throws IOException {
        Utils.ioResult(
            writeControlRecord()
                .thenCompose(ignored -> writer.asyncClose())
                .thenCompose(ignored -> dlm.asyncClose()));
    }
}
