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
package org.apache.bookkeeper.common.util.nativelib;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

/**
 * Utility class to load jni library from inside a JAR.
 */
@UtilityClass
public class NativeUtils {

    public static final String OS_NAME = System.getProperty("os.name").toLowerCase(Locale.US);
    public static final String TEMP_WORKDIR_PROPERTY_NAME = "org.apache.bookkeeper.native.workdir";

    private static final String TEMP_DIR_NAME = "native";

    /**
     * loads given library from the this jar. ie: this jar contains: /lib/pulsar-checksum.jnilib
     *
     * @param path
     *            : absolute path of the library in the jar <br/>
     *            if this jar contains: /lib/pulsar-checksum.jnilib then provide the same absolute path as input
     * @throws Exception
     */

    public static void loadLibraryFromJar(String path) throws Exception {
        checkArgument(path.startsWith("/"), "absolute path must start with /");

        String[] parts = path.split("/");
        checkArgument(parts.length > 0, "absolute path must contain file name");

        String filename = parts[parts.length - 1];

        // create the temp dir
        final Path dir;
        final String tempWorkDirName = System.getProperty(TEMP_WORKDIR_PROPERTY_NAME);
        if (tempWorkDirName == null || tempWorkDirName.isEmpty()) {
            dir = Files.createTempDirectory(TEMP_DIR_NAME);
        } else {
            final File tempWorkDir = new File(tempWorkDirName);
            if (!tempWorkDir.exists() || !tempWorkDir.isDirectory()) {
                throw new FileNotFoundException("The tempWorkDir doesn't exist: " + tempWorkDirName);
            }
            dir = Files.createTempDirectory(tempWorkDir.toPath(), TEMP_DIR_NAME);
        }
        dir.toFile().deleteOnExit();

        // create the temp file
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

    /**
     * Returns jni library extension based on OS specification. Maven-nar generates jni library based on different OS :
     * http://mark.donszelmann.org/maven-nar-plugin/aol.html (jni.extension)
     *
     * @return library type
     */
    public static String libType() {
        if (OS_NAME.indexOf("mac") >= 0) {
            return "jnilib";
        } else if (OS_NAME.indexOf("nix") >= 0 || OS_NAME.indexOf("nux") >= 0 || OS_NAME.indexOf("aix") > 0) {
            return "so";
        } else if (OS_NAME.indexOf("win") >= 0) {
            return "dll";
        }
        throw new TypeNotPresentException(OS_NAME + " not supported", null);
    }

    public static void checkArgument(boolean expression, @NonNull Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }
}
