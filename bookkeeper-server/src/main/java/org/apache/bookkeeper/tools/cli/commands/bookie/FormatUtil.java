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
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.LedgerIdFormatter;

/**
 * .Provide to format message.
 */
public class FormatUtil {

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

        System.out.println(
            "--------- Lid=" + ledgerIdFormatter.formatLedgerId(ledgerId) + ", Eid=" + entryId + ", ByteOffset=" + pos
                + ", EntrySize=" + entrySize + " ---------");
        if (entryId == Bookie.METAENTRY_ID_LEDGER_KEY) {
            int masterKeyLen = recBuff.readInt();
            byte[] masterKey = new byte[masterKeyLen];
            recBuff.readBytes(masterKey);
            System.out.println("Type:           META");
            System.out.println("MasterKey:      " + bytes2Hex(masterKey));
            System.out.println();
            return;
        }
        if (entryId == Bookie.METAENTRY_ID_FENCE_KEY) {
            System.out.println("Type:           META");
            System.out.println("Fenced");
            System.out.println();
            return;
        }
        // process a data entry
        long lastAddConfirmed = recBuff.readLong();
        System.out.println("Type:           DATA");
        System.out.println("LastConfirmed:  " + lastAddConfirmed);
        if (!printMsg) {
            System.out.println();
            return;
        }
        // skip digest checking
        recBuff.skipBytes(8);
        System.out.println("Data:");
        System.out.println();
        try {
            byte[] ret = new byte[recBuff.readableBytes()];
            recBuff.readBytes(ret);
            entryFormatter.formatEntry(ret);
        } catch (Exception e) {
            System.out.println("N/A. Corrupted.");
        }
        System.out.println();
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
