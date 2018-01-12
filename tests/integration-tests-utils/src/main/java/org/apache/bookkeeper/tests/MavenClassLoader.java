/**
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
package org.apache.bookkeeper.tests;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import org.jboss.shrinkwrap.resolver.api.maven.Maven;

public class MavenClassLoader implements AutoCloseable {

    private MavenClassLoader(URLClassLoader cl) {
        this.classloader = cl;
    }

    private final URLClassLoader classloader;

    public static MavenClassLoader forArtifact(String mainArtifact) throws Exception {
        File[] files = Maven.resolver().resolve(mainArtifact)
            .withTransitivity().asFile();
        URLClassLoader cl = new URLClassLoader(Arrays.stream(files)
                                               .map((f) -> {
                                                       try {
                                                           return f.toURI().toURL();
                                                       } catch (Throwable t) {
                                                           throw new RuntimeException(t);
                                                       }
                                                   })
                                               .toArray(URL[]::new));
        return new MavenClassLoader(cl);
    }

    public static MavenClassLoader forBookKeeperVersion(String version) throws Exception {
        return forArtifact("org.apache.bookkeeper:bookkeeper-server:" +  version);
    }

    public Object newInstance(String className, Object... args) throws Exception {
        Class<?> klass = Class.forName(className, true, classloader);
        return klass.getConstructor(Arrays.stream(args).map((a)-> a.getClass()).toArray(Class[]::new))
            .newInstance(args);
    }

    public Object newBookKeeper(String zookeeper) throws Exception {
        return newInstance("org.apache.bookkeeper.client.BookKeeper", zookeeper);
    }

    public Object digestType(String type) throws Exception {
        String className = "org.apache.bookkeeper.client.BookKeeper$DigestType";
        for (Object o : classloader.loadClass(className).getEnumConstants()) {
            if (o.toString().equals(type)) {
                return o;
            }
        }
        throw new ClassNotFoundException("No such digest type " + type);
    }

    public void close() throws Exception {
        classloader.close();
    }
}
