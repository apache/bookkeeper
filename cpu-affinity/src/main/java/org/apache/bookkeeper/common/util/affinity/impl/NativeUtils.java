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
package org.apache.bookkeeper.common.util.affinity.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

/**
 * Utility class to load jni library from inside a JAR.
 */
@UtilityClass
public class NativeUtils {
    /**
     * loads given library from the this jar. ie: this jar contains: /lib/pulsar-checksum.jnilib
     *
     * @param path
     *            : absolute path of the library in the jar <br/>
     *            if this jar contains: /lib/pulsar-checksum.jnilib then provide the same absolute path as input
     * @throws Exception
     */
    @SuppressFBWarnings(
            value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE",
            justification = "work around for java 9: https://github.com/spotbugs/spotbugs/issues/493")
    public static void loadLibraryFromJar(String path) throws Exception {
        checkArgument(path.startsWith("/"), "absolute path must start with /");

        String[] parts = path.split("/");
        checkArgument(parts.length > 0, "absolute path must contain file name");

        String filename = parts[parts.length - 1];
        checkArgument(path.startsWith("/"), "absolute path must start with /");

        Path dir = Files.createTempDirectory("native");
        dir.toFile().deleteOnExit();
        File temp = new File(dir.toString(), filename);
        temp.deleteOnExit();

        byte[] buffer = new byte[1024];
        int read;

        try (InputStream input = NativeUtils.class.getResourceAsStream(path);
                OutputStream out = new FileOutputStream(temp)) {
            if (input == null) {
                throw new FileNotFoundException("Couldn't find file into jar " + path);
            }

            while ((read = input.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
        }

        if (!temp.exists()) {
            throw new FileNotFoundException("Failed to copy file from jar at " + temp.getAbsolutePath());
        }

        System.load(temp.getAbsolutePath());
    }

    private static void checkArgument(boolean expression, @NonNull Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }
}
