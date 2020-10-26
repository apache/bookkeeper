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

package org.apache.bookkeeper.proto.checksum;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code SHA-1} based digest manager.
 *
 * <p>NOTE: This class is tended to be used by this project only. External users should not rely on it directly.
 */
public class MacDigestManager extends DigestManager {
    private static final Logger LOG = LoggerFactory.getLogger(MacDigestManager.class);

    public static final String DIGEST_ALGORITHM = "SHA-1";
    public static final String KEY_ALGORITHM = "HmacSHA1";

    public static final int MAC_CODE_LENGTH = 20;

    final byte[] passwd;

    static final byte[] EMPTY_LEDGER_KEY;
    static {
        try {
            EMPTY_LEDGER_KEY = MacDigestManager.genDigest("ledger", new byte[0]);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private final ThreadLocal<Mac> mac = new ThreadLocal<Mac>() {
        @Override
        protected Mac initialValue() {
            try {
                byte[] macKey = genDigest("mac", passwd);
                SecretKeySpec keySpec = new SecretKeySpec(macKey, KEY_ALGORITHM);
                Mac mac = Mac.getInstance(KEY_ALGORITHM);
                mac.init(keySpec);
                return mac;
            } catch (GeneralSecurityException gse) {
                LOG.error("Couldn't not get mac instance", gse);
                return null;
            }
        }
    };

    public MacDigestManager(long ledgerId, byte[] passwd, boolean useV2Protocol, ByteBufAllocator allocator)
            throws GeneralSecurityException {
        super(ledgerId, useV2Protocol, allocator);
        this.passwd = Arrays.copyOf(passwd, passwd.length);
    }

    public static byte[] genDigest(String pad, byte[] passwd) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance(DIGEST_ALGORITHM);
        digest.update(pad.getBytes(UTF_8));
        digest.update(passwd);
        return digest.digest();
    }

    @Override
    int getMacCodeLength() {
        return MAC_CODE_LENGTH;
    }


    @Override
    void populateValueAndReset(ByteBuf buffer) {
        buffer.writeBytes(mac.get().doFinal());
    }

    @Override
    void update(ByteBuf data) {
        mac.get().update(data.nioBuffer());
    }


}
