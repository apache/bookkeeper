/*
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
package org.apache.bookkeeper.bookie.storage.directentrylogger;

/**
 * Events.
 */
public enum Events {
    /**
     * Fallocate is not available on this host. This generally indicates that the process is running on a
     * non-Linux operating system. The lack of fallocate means that the filesystem will have to do more
     * bookkeeping as data is written to the file, which will slow down writes.
     */
    FALLOCATE_NOT_AVAILABLE,

    /**
     * EntryLog ID candidates selected. These are the set entry log ID that subsequent entry log files
     * will use. To find the candidates, the bookie lists all the log ids which have already been used,
     * and finds the longest contiguous block of free ids. Over the lifetime of a bookie, a log id can
     * be reused. This is not a problem, as the ids are only referenced from the index, and an
     * entry log file will not be deleted if there are still references to it in the index.
     * Generally candidates are selected at bookie boot, but they may also be selected at a later time
     * if the current set of candidates is depleted.
     */
    ENTRYLOG_IDS_CANDIDATES_SELECTED,

    /**
     * The entrylogger({@link org.apache.bookkeeper.bookie.storage.EntryLogger}) has been created.
     * This occurs during bookie bootup, and the same  entry logger will be used for the duration of
     * the bookie process's lifetime.
     */
    ENTRYLOGGER_CREATED,

    /**
     * The entrylogger has been configured in a way that will likely result in errors during operation.
     */
    ENTRYLOGGER_MISCONFIGURED,

    /**
     * The entrylogger has started writing a new log file. The previous log file may not
     * be entirely flushed when this is called, though they will be after an explicit flush call.
     */
    LOG_ROLL,

    /**
     * A log file has been deleted. This happens as a result of GC, when all entries in the file
     * belong to deleted ledgers, or compaction, where the live entries have been copied to a new
     * log.
     */
    LOG_DELETED,

    /**
     * An error occurred closing an entrylog reader. This is non-fatal but it may leak the file handle
     * and the memory buffer of the reader in question.
     */
    READER_CLOSE_ERROR,

    /**
     * An attempt to read entrylog metadata failed. Falling back to scanning the log to get the metadata.
     * This can occur if a bookie crashes before closing the entrylog cleanly.
     */
    READ_METADATA_FALLBACK,

    /**
     * A new entrylog has been created. The filename has the format [dstLogId].compacting, where dstLogId is
     * a new unique log ID. Entrylog compaction will copy live entries from an existing src log to this new
     * compacting destination log. There is a 1-1 relationship between source logs and destination log logs.
     * Once the copy completes, the compacting log will be marked as compacted by renaming the file to
     * [dstLogId].log.[srcLogId].compacted, where srcLogId is the ID of the entrylog from which the live entries
     * were copied. A new hardlink, [dstLogId].log, is created to point to the same inode, making the entry
     * log available to be read. The compaction algorithm then updates the index with the offsets of the entries
     * in the compacted destination log. Once complete, the index is flushed and all intermediate files (links)
     * are deleted along with the original source log file.
     * The entry copying phase of compaction is expensive. The renaming and linking in the algorithm exists so
     * if a failure occurs after copying has completed, the work will not need to be redone.
     */
    COMPACTION_LOG_CREATED,

    /**
     * A partially compacted log has been recovered. The log file is of the format [dstLogId].log.[srcLogId].compacted.
     * The log will be scanned and the index updated with the offsets of the entries in the log. Once complete, the
     * log with ID srcLogId is deleted.
     * <p/>
     * See {@link #COMPACTION_LOG_CREATED} for more information on compaction.
     */
    COMPACTION_LOG_RECOVERED,

    /**
     * A compaction log has been marked as compacted. A log is marked as compacted by renaming from [dstLogId].log to
     * [dstLogId].log.[srcLogId].compacted. All live entries from the src log have been successfully copied to the
     * destination log, at this point.
     * <p/>
     * See {@link #COMPACTION_LOG_CREATED} for more information on compaction.
     */
    COMPACTION_MARK_COMPACTED,

    /**
     * A compacted log has been made available for reads. A log is made available by creating a hardlink
     * pointing from [dstLogId].log, to [dstLogId].log.[srcLogId].compacted. These files, pointing to the
     * same inode, will continue to exist until the compaction operation is complete.
     * <p/>
     * A reader with a valid offset will now be able to read from this log, so the index can be updated.
     * <p/>
     * See {@link #COMPACTION_LOG_CREATED} for more information on compaction.
     */
    COMPACTION_MAKE_AVAILABLE,

    /**
     * Compaction has been completed for a log.
     * All intermediatory files are deleted, along with the src entrylog file.
     * <p/>
     * See {@link #COMPACTION_LOG_CREATED} for more information on compaction.
     */
    COMPACTION_COMPLETE,

    /**
     * Failed to delete files while aborting a compaction operation. While this is not fatal, it
     * can mean that there are issues writing to the filesystem that need to be investigated.
     */
    COMPACTION_ABORT_EXCEPTION,

    /**
     * Failed to delete files while completing a compaction operation. While this is not fatal, it
     * can mean that there are issues writing to the filesystem that need to be investigated.
     */
    COMPACTION_DELETE_FAILURE,
}
