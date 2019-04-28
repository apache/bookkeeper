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
package org.apache.distributedlog.api.namespace;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.acl.AccessControlManager;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.callback.NamespaceListener;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.InvalidStreamNameException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.namespace.NamespaceDriver;

/**
 * A namespace is the basic unit for managing a set of distributedlogs.
 *
 * <h4>Namespace Interface</h4>
 *
 * <P>
 * The <code>Namespace</code> interface is implemented by different backend providers.
 * There are several components are required for an implementation:
 * <OL>
 *     <LI>Log Management -- manage logs in a given namespace. e.g. create/open/delete log, list of logs,
 *         watch the changes of logs.
 *     <LI>Access Control -- manage the access controls for logs in the namespace.
 * </OL>
 * </P>
 *
 * <h4>Namespace Location</h4>
 *
 * <p>At the highest level, a <code>Namespace</code> is located by a <code>URI</code>. The location
 * URI is in string form has the syntax</p>
 *
 * <blockquote>
 * distributedlog[<tt><b>-</b></tt><i>provider</i>]<tt><b>:</b></tt><i>provider-specific-path</i>
 * </blockquote>
 *
 * <p>where square brackets [...] delineate optional components and the characters
 * <tt><b>-</b></tt> and <tt><b>:</b></tt>
 * stand for themselves.</p>
 *
 * <p>The <code>provider</code> part in the URI indicates what is the backend used for this namespace. For example:
 * <i>distributedlog-bk</i> URI is storing logs in bookkeeper, while <i>distributedlog-mem</i> URI is storing logs in
 * memory. The <code>provider</code> part is optional. It would use bookkeeper backend if the <i>provider</i> part
 * is omitted.</p>
 *
 * @see DistributedLogManager
 * @since 0.3.32
 */
@Public
@Evolving
public interface Namespace extends AutoCloseable{

    /**
     * Get the namespace driver used by this namespace.
     *
     * @return namespace driver
     */
    NamespaceDriver getNamespaceDriver();

    //
    // Method to operate logs
    //

    /**
     * Create a log named <i>logName</i>.
     *
     * @param logName
     *          name of the log
     * @throws InvalidStreamNameException if log name is invalid.
     * @throws IOException when encountered issues with backend.
     */
    void createLog(String logName)
            throws InvalidStreamNameException, IOException;

    /**
     * Delete a log named <i>logName</i>.
     *
     * @param logName
     *          name of the log
     * @throws InvalidStreamNameException if log name is invalid
     * @throws LogNotFoundException if log doesn't exist
     * @throws IOException when encountered issues with backend
     */
    void deleteLog(String logName)
            throws InvalidStreamNameException, LogNotFoundException, IOException;

    /**
     * Rename a log from <i>oldName</i> to <i>newName</i>.
     *
     * @param oldName old log name
     * @param newName new log name
     * @return a future represents the rename result.
     * @throws InvalidStreamNameException if log name is invalid
     * @throws LogNotFoundException if old log doesn't exist
     * @throws org.apache.distributedlog.exceptions.LogExistsException if the new log exists
     * @throws IOException when encountered issues with backend.
     */
    CompletableFuture<Void> renameLog(String oldName, String newName);

    /**
     * Open a log named <i>logName</i>.
     * A distributedlog manager is returned to access log <i>logName</i>.
     *
     * @param logName
     *          name of the log
     * @return distributedlog manager instance.
     * @throws InvalidStreamNameException if log name is invalid.
     * @throws LogNotFoundException if log doesn't exist.
     * @throws IOException when encountered issues with backend.
     */
    DistributedLogManager openLog(String logName)
            throws InvalidStreamNameException, LogNotFoundException, IOException;

    /**
     * Open a log named <i>logName</i> with specific log configurations.
     *
     * <p>This method allows the caller to override global configuration settings by
     * supplying log configuration overrides. Log config overrides come in two flavors,
     * static and dynamic. Static config never changes in the lifecyle of <code>DistributedLogManager</code>,
     * dynamic config changes by reloading periodically and safe to access from any context.</p>
     *
     * @param logName
     *          name of the log
     * @param logConf
     *          static log configuration
     * @param dynamicLogConf
     *          dynamic log configuration
     * @return distributedlog manager instance.
     * @throws InvalidStreamNameException if log name is invalid.
     * @throws LogNotFoundException if log doesn't exist.
     * @throws IOException when encountered issues with backend.
     */
    DistributedLogManager openLog(String logName,
                                  Optional<DistributedLogConfiguration> logConf,
                                  Optional<DynamicDistributedLogConfiguration> dynamicLogConf,
                                  Optional<StatsLogger> perStreamStatsLogger)
            throws InvalidStreamNameException, LogNotFoundException, IOException;

    /**
     * Check whether the log <i>logName</i> exist.
     *
     * @param logName
     *          name of the log
     * @return <code>true</code> if the log exists, otherwise <code>false</code>.
     * @throws IOException when encountered exceptions on checking
     */
    boolean logExists(String logName)
            throws IOException;

    /**
     * Retrieve the logs under the namespace.
     *
     * @return iterator of the logs under the namespace.
     * @throws IOException when encountered issues with backend.
     */
    Iterator<String> getLogs()
            throws IOException;

    /**
     * Retrieve the logs under a given <i>logNamePrefix</i>.
     *
     * @param logNamePrefix log name prefix
     * @return iterator of the logs under the log name prefix
     * @throws IOException when encountered issues with backend.
     */
    Iterator<String> getLogs(String logNamePrefix)
            throws IOException;

    //
    // Methods for namespace
    //

    /**
     * Register namespace listener on stream updates under the namespace.
     *
     * @param listener
     *          listener to receive stream updates under the namespace
     */
    void registerNamespaceListener(NamespaceListener listener);

    /**
     * Create an access control manager to manage/check acl for logs.
     *
     * @return access control manager for logs under the namespace.
     * @throws IOException
     */
    AccessControlManager createAccessControlManager()
            throws IOException;

    /**
     * Close the namespace.
     */
    @Override
    void close();

}
