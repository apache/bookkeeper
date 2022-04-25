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
package org.apache.bookkeeper.tools.cli.commands.bookie;

import io.netty.buffer.ByteBuf;
import java.util.Formatter;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * .Provide to format message.
 */
public class FormatUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FormatUtil.class);

    /**
     * Format the message into a readable format.
     * @param pos
     *          File offset of the message stored in entry log file
     * @param recBuff
     *          Entry Data
     * @param printMsg
     *          Whether printing the message body
     * @param ledgerIdFormatter
     * @param entryFormatter
     */
    public static void formatEntry(long pos, ByteBuf recBuff, boolean printMsg, LedgerIdFormatter ledgerIdFormatter,
                                   EntryFormatter entryFormatter) {
        int entrySize = recBuff.readableBytes();
        long ledgerId = recBuff.readLong();
        long entryId = recBuff.readLong();

        LOG.info("--------- Lid={}, Eid={}, ByteOffset={}, EntrySize={} ---------",
            ledgerIdFormatter.formatLedgerId(ledgerId), entryId, pos, entrySize);
        if (entryId == BookieImpl.METAENTRY_ID_LEDGER_KEY) {
            int masterKeyLen = recBuff.readInt();
            byte[] masterKey = new byte[masterKeyLen];
            recBuff.readBytes(masterKey);
            LOG.info("Type:           META");
            LOG.info("MasterKey:      {}", bytes2Hex(masterKey));
            LOG.info("");
            return;
        }
        if (entryId == BookieImpl.METAENTRY_ID_FENCE_KEY) {
            LOG.info("Type:           META");
            LOG.info("Fenced");
            LOG.info("");
            return;
        }
        // process a data entry
        long lastAddConfirmed = recBuff.readLong();
        LOG.info("Type:           DATA");
        LOG.info("LastConfirmed:  {}", lastAddConfirmed);
        if (!printMsg) {
            LOG.info("");
            return;
        }
        // skip digest checking
        recBuff.skipBytes(8);
        LOG.info("Data:");
        LOG.info("");
        try {
            byte[] ret = new byte[recBuff.readableBytes()];
            recBuff.readBytes(ret);
            entryFormatter.formatEntry(ret);
        } catch (Exception e) {
            LOG.info("N/A. Corrupted.");
        }
        LOG.info("");
    }

    public static String bytes2Hex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        Formatter formatter = new Formatter(sb);
        for (byte b : data) {
            formatter.format("%02x", b);
        }
        formatter.close();
        return sb.toString();
    }
}
