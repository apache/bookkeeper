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
package org.apache.distributedlog.fs;

import com.google.common.collect.Lists;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.DistributedLogConstants;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.exceptions.LogEmptyException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A FileSystem Implementation powered by replicated logs.
 */
@Slf4j
public class DLFileSystem extends FileSystem {

    //
    // Settings
    //

    public static final String DLFS_CONF_FILE = "dlog.configuration.file";


    private URI rootUri;
    private Namespace namespace;
    private final DistributedLogConfiguration dlConf;
    private Path workingDir;

    public DLFileSystem() {
        this.dlConf = new DistributedLogConfiguration();
        setWorkingDirectory(new Path(System.getProperty("user.dir", "")));
    }

    @Override
    public URI getUri() {
        return rootUri;
    }

    //
    // Initialization
    //

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);

        // initialize

        this.rootUri = name;
        // load the configuration
        String dlConfLocation = conf.get(DLFS_CONF_FILE);
        if (null != dlConfLocation) {
            try {
                this.dlConf.loadConf(new File(dlConfLocation).toURI().toURL());
                log.info("Loaded the distributedlog configuration from {}", dlConfLocation);
            } catch (ConfigurationException e) {
                log.error("Failed to load the distributedlog configuration from " + dlConfLocation, e);
                throw new IOException("Failed to load distributedlog configuration from " + dlConfLocation);
            }
        }
        log.info("Initializing the filesystem at {}", name);
        // initialize the namespace
        this.namespace = NamespaceBuilder.newBuilder()
                .clientId("dlfs-client-" + InetAddress.getLocalHost().getHostName())
                .conf(dlConf)
                .regionId(DistributedLogConstants.LOCAL_REGION_ID)
                .uri(name)
                .build();
        log.info("Initialized the filesystem at {}", name);
    }

    @Override
    public void close() throws IOException {
        // clean up the resource
        namespace.close();
        super.close();
    }

    //
    // Util Functions
    //

    private Path makeAbsolute(Path f) {
        if (f.isAbsolute()) {
            return f;
        } else {
            return new Path(workingDir, f);
        }
    }

    private String getStreamName(Path relativePath) {
        return makeAbsolute(relativePath).toUri().getPath().substring(1);
    }

    //
    // Home & Working Directory
    //

    @Override
    public Path getHomeDirectory() {
        return this.makeQualified(new Path(System.getProperty("user.home", "")));
    }

    protected Path getInitialWorkingDirectory() {
        return this.makeQualified(new Path(System.getProperty("user.dir", "")));
    }

    @Override
    public void setWorkingDirectory(Path path) {
        workingDir = makeAbsolute(path);
        checkPath(workingDir);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }


    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException {
        try {
            DistributedLogManager dlm = namespace.openLog(getStreamName(path));
            LogReader reader;
            try {
                reader = dlm.openLogReader(DLSN.InitialDLSN);
            } catch (LogNotFoundException lnfe) {
                throw new FileNotFoundException(path.toString());
            } catch (LogEmptyException lee) {
                throw new FileNotFoundException(path.toString());
            }
            return new FSDataInputStream(
                new BufferedFSInputStream(
                    new DLInputStream(dlm, reader, 0L),
                    bufferSize));
        } catch (LogNotFoundException e) {
            throw new FileNotFoundException(path.toString());
        }
    }

    @Override
    public FSDataOutputStream create(Path path,
                                     FsPermission fsPermission,
                                     boolean overwrite,
                                     int bufferSize,
                                     short replication,
                                     long blockSize,
                                     Progressable progressable) throws IOException {
        // for overwrite, delete the existing file first.
        if (overwrite) {
            delete(path, false);
        }

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(dlConf);
        confLocal.setEnsembleSize(replication);
        confLocal.setWriteQuorumSize(replication);
        confLocal.setAckQuorumSize(replication);
        confLocal.setMaxLogSegmentBytes(blockSize);
        return append(path, bufferSize, Optional.of(confLocal));
    }

    @Override
    public FSDataOutputStream append(Path path,
                                     int bufferSize,
                                     Progressable progressable) throws IOException {
        return append(path, bufferSize, Optional.empty());
    }

    private FSDataOutputStream append(Path path,
                                      int bufferSize,
                                      Optional<DistributedLogConfiguration> confLocal)
            throws IOException {
        try {
            DistributedLogManager dlm = namespace.openLog(
                getStreamName(path),
                confLocal,
                Optional.empty(),
                Optional.empty());
            AsyncLogWriter writer = Utils.ioResult(dlm.openAsyncLogWriter());
            return new FSDataOutputStream(
                new BufferedOutputStream(
                    new DLOutputStream(dlm, writer), bufferSize
                ),
                statistics,
                writer.getLastTxId() < 0L ? 0L : writer.getLastTxId());
        } catch (LogNotFoundException le) {
            throw new FileNotFoundException(path.toString());
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        try {
            String logName = getStreamName(path);
            if (recursive) {
                Iterator<String> logs = namespace.getLogs(logName);
                while (logs.hasNext()) {
                    String child = logs.next();
                    Path childPath = new Path(path, child);
                    delete(childPath, recursive);
                }
            }
            namespace.deleteLog(logName);
            return true;
        } catch (LogNotFoundException e) {
            return true;
        }
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        String logName = getStreamName(path);
        try {
            Iterator<String> logs = namespace.getLogs(logName);
            List<FileStatus> statusList = Lists.newArrayList();
            while (logs.hasNext()) {
                String child = logs.next();
                Path childPath = new Path(path, child);
                statusList.add(getFileStatus(childPath));
            }
            Collections.sort(statusList, Comparator.comparing(fileStatus -> fileStatus.getPath().getName()));
            return statusList.toArray(new FileStatus[statusList.size()]);
        } catch (LogNotFoundException e) {
            throw new FileNotFoundException(path.toString());
        }
    }


    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        String streamName = getStreamName(path);

        // Create a dummy stream to make the path exists.
        namespace.createLog(streamName);
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        String logName = getStreamName(path);
        boolean exists = namespace.logExists(logName);
        if (!exists) {
            throw new FileNotFoundException(path.toString());
        }

        long endPos;
        try {
            DistributedLogManager dlm = namespace.openLog(logName);
            endPos = dlm.getLastTxId();
        } catch (LogNotFoundException e) {
            throw new FileNotFoundException(path.toString());
        } catch (LogEmptyException e) {
            endPos = 0L;
        }

        // we need to store more metadata information on logs for supporting filesystem-like use cases
        return new FileStatus(
            endPos,
            false,
            3,
            dlConf.getMaxLogSegmentBytes(),
            0L,
            makeAbsolute(path));
    }


    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        String srcLog = getStreamName(src);
        String dstLog = getStreamName(dst);
        try {
            namespace.renameLog(srcLog, dstLog).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DLInterruptedException("Interrupted at renaming " + srcLog + " to " + dstLog, e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException("Failed to rename " + srcLog + " to " + dstLog, e.getCause());
            }
        }
        return true;
    }

    //
    // Not Supported
    //

    @Override
    public boolean truncate(Path f, long newLength) throws IOException {
        throw new UnsupportedOperationException("Truncate is not supported yet");
    }
}
