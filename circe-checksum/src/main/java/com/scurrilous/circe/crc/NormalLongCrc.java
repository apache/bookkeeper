/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.scurrilous.circe.crc;

/**
 * Implements a "normal" MSB-first long-width CRC function using a lookup table.
 * Does not support bit-widths less than 8.
 */
final class NormalLongCrc extends AbstractLongCrc {

    private final long widthMask;
    private final long[] table = new long[256];

    NormalLongCrc(String algorithm, int bitWidth, long poly, long init, long xorOut) {
        super(algorithm, bitWidth, init, xorOut);
        if (bitWidth < 8)
            throw new IllegalArgumentException("invalid CRC width");

        widthMask = bitWidth < 64 ? ((1L << bitWidth) - 1) : ~0L;
        final long top = 1L << (bitWidth - 1);
        for (int i = 0; i < 256; ++i) {
            long crc = (long) i << (bitWidth - 8);
            for (int j = 0; j < 8; ++j)
                crc = (crc & top) != 0 ? (crc << 1) ^ poly : crc << 1;
            table[i] = crc & widthMask;
        }
    }

    @Override
    protected long resumeRaw(long crc, byte[] input, int index, int length) {
        for (int i = 0; i < length; ++i)
            crc = table[(int) ((crc >>> (bitWidth - 8)) ^ input[index + i]) & 0xff] ^ (crc << 8);
        return crc & widthMask;
    }
}
