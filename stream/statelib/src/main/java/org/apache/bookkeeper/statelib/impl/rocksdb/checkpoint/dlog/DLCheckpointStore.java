/*
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
 */
package org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.dlog;

import com.google.common.collect.Lists;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;
import org.apache.bookkeeper.statelib.impl.Constants;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.bk.LedgerMetadata;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.exceptions.LogEmptyException;
import org.apache.distributedlog.exceptions.LogExistsException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.util.Utils;

/**
 * Dlog based checkpoint store.
 */
@Slf4j
public class DLCheckpointStore implements CheckpointStore {

    private final Namespace namespace;

    public DLCheckpointStore(Namespace namespace) {
        this.namespace = namespace;
    }

    @Override
    public List<String> listFiles(String filePath) throws IOException {
        return Lists.newArrayList(namespace.getLogs(filePath));
    }

    @Override
    public boolean fileExists(String filePath) throws IOException {
        return namespace.logExists(filePath);
    }

    @Override
    public long getFileLength(String filePath) throws IOException {
        try (DistributedLogManager dlm = namespace.openLog(filePath)) {
            return dlm.getLastTxId();
        } catch (LogNotFoundException e) {
            throw new FileNotFoundException(filePath);
        } catch (LogEmptyException e) {
            return 0;
        }
    }

    @Override
    public InputStream openInputStream(String filePath) throws IOException {
        try {
            DistributedLogManager dlm = namespace.openLog(filePath);
            LogReader reader;
            try {
                reader = dlm.openLogReader(DLSN.InitialDLSN);
            } catch (LogNotFoundException | LogEmptyException e) {
                throw new FileNotFoundException(filePath);
            }
            return new BufferedInputStream(
                new DLInputStream(dlm, reader, 0L), 128 * 1024);
        } catch (LogNotFoundException e) {
            throw new FileNotFoundException(filePath);
        }
    }

    @Override
    public OutputStream openOutputStream(String filePath) throws IOException {
        try {
            DistributedLogManager dlm = namespace.openLog(
                filePath);
            LedgerMetadata metadata = new LedgerMetadata();
            metadata.setApplication(Constants.LEDGER_METADATA_APPLICATION_STREAM_STORAGE);
            metadata.setComponent("checkpoint-store");
            AsyncLogWriter writer = Utils.ioResult(dlm.openAsyncLogWriter(metadata));
            return new BufferedOutputStream(
                new DLOutputStream(dlm, writer), 128 * 1024);
        } catch (LogNotFoundException le) {
            throw new FileNotFoundException(filePath);
        }
    }

    @Override
    public void rename(String srcLog, String dstLog) throws IOException {
        log.info("Renaming {} to {}", srcLog, dstLog);
        try {
            namespace.renameLog(srcLog, dstLog).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DLInterruptedException("Interrupted at renaming " + srcLog + " to " + dstLog, e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof LogExistsException) {
                throw new FileAlreadyExistsException("Dest file already exists : " + dstLog);
            } else if (e.getCause() instanceof LogNotFoundException) {
                throw new NoSuchFileException("Src file or dest directory is not found");
            } else if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException("Failed to rename " + srcLog + " to " + dstLog, e.getCause());
            }
        }
    }

    @Override
    public void deleteRecursively(String srcPath) throws IOException {
        Iterator<String> logs = namespace.getLogs(srcPath);
        while (logs.hasNext()) {
            String child = logs.next();
            deleteRecursively(srcPath + "/" + child);
        }
        namespace.deleteLog(srcPath);
    }

    @Override
    public void delete(String srcPath) throws IOException {
        namespace.deleteLog(srcPath);
    }

    @Override
    public void createDirectories(String srcPath) throws IOException {
        namespace.createLog(srcPath);
    }

    @Override
    public void close() {
        namespace.close();
    }
}
