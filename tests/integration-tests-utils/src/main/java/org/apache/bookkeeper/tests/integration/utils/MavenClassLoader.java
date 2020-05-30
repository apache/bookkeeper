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
package org.apache.bookkeeper.tests.integration.utils;

import com.google.common.collect.Lists;

import groovy.lang.Closure;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.jboss.shrinkwrap.resolver.api.maven.ConfigurableMavenResolverSystem;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.ScopeType;
import org.jboss.shrinkwrap.resolver.api.maven.coordinate.MavenDependencies;
import org.jboss.shrinkwrap.resolver.api.maven.coordinate.MavenDependency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A maven class loader for resolving and loading maven artifacts.
 */
public class MavenClassLoader implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MavenClassLoader.class);

    private MavenClassLoader(URLClassLoader cl) {
        this.classloader = cl;
    }

    private final URLClassLoader classloader;

    public static MavenClassLoader forArtifact(String repo, String mainArtifact) throws Exception {
        return createClassLoader(Maven.configureResolver().withRemoteRepo("custom", repo, "default"),
                                 mainArtifact);
    }

    public static MavenClassLoader forArtifact(String mainArtifact) throws Exception {
        return createClassLoader(Maven.configureResolver(), mainArtifact);
    }

    private static MavenClassLoader createClassLoader(ConfigurableMavenResolverSystem resolver,
                                                      String mainArtifact) throws Exception {
        Optional<String> slf4jVersion = Arrays.stream(resolver.resolve(mainArtifact)
                                                      .withTransitivity().asResolvedArtifact())
            .filter((a) -> a.getCoordinate().getGroupId().equals("org.slf4j")
                    && a.getCoordinate().getArtifactId().equals("slf4j-log4j12"))
            .map((a) -> a.getCoordinate().getVersion())
            .findFirst();

        List<MavenDependency> deps = Lists.newArrayList(
                MavenDependencies.createDependency(
                        mainArtifact, ScopeType.COMPILE, false,
                        MavenDependencies.createExclusion("org.slf4j:slf4j-log4j12"),
                        MavenDependencies.createExclusion("log4j:log4j")));
        if (slf4jVersion.isPresent()) {
            deps.add(MavenDependencies.createDependency("org.slf4j:slf4j-simple:" + slf4jVersion.get(),
                                                        ScopeType.COMPILE, false));
        }

        File[] files = resolver.addDependencies(deps.toArray(new MavenDependency[0]))
            .resolve().withTransitivity().asFile();
        URLClassLoader cl = AccessController.doPrivileged(
                new PrivilegedAction<URLClassLoader>() {
                    @Override
                    public URLClassLoader run() {
                        return new URLClassLoader(Arrays.stream(files)
                                                  .map((f) -> {
                                                          try {
                                                              return f.toURI().toURL();
                                                          } catch (Throwable t) {
                                                              throw new RuntimeException(t);
                                                          }
                                                      })
                                                  .toArray(URL[]::new),
                                                  ClassLoader.getSystemClassLoader());
                    }
                });
        return new MavenClassLoader(cl);
    }

    public static MavenClassLoader forBookKeeperVersion(String version) throws Exception {
        return forArtifact("org.apache.bookkeeper:bookkeeper-server:" +  version);
    }

    public Object getClass(String className) throws Exception {
        return Class.forName(className, true, classloader);
    }

    public Object callStaticMethod(String className, String methodName, ArrayList<?> args) throws Exception {
        Class<?> klass = Class.forName(className, true, classloader);

        try {
            Class<?>[] paramTypes = args.stream().map((a)-> a.getClass()).toArray(Class[]::new);
            return klass.getMethod(methodName, paramTypes).invoke(null, args.stream().toArray(Object[]::new));
        } catch (NoSuchMethodException nsme) {
            // maybe the params are primitives
            Class<?>[] paramTypes = args.stream().map((a) -> {
                    Class<?> k = a.getClass();
                    try {
                        Object type = k.getField("TYPE").get(null);
                        if (type instanceof Class<?>) {
                            return (Class<?>) type;
                        } else {
                            return k;
                        }
                    } catch (IllegalAccessException | NoSuchFieldException nsfe) {
                        return k;
                    }
                }).toArray(Class[]::new);
            return klass.getMethod(methodName, paramTypes).invoke(null, args.stream().toArray(Object[]::new));
        }
    }

    public Object createCallback(String interfaceName, Closure closure) throws Exception {
        final Constructor<MethodHandles.Lookup> constructor = MethodHandles.Lookup.class.getDeclaredConstructor(
                Class.class, int.class);
        constructor.setAccessible(true);
        return Proxy.newProxyInstance(classloader,
                                      new Class<?>[]{ Class.forName(interfaceName, true, classloader) },
                                      new InvocationHandler() {

                                          @Override
                                          public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
                                              if (args.length == closure.getMaximumNumberOfParameters()) {
                                                  return closure.call(args);
                                              } else {
                                                  final Class<?> declaringClass = m.getDeclaringClass();
                                                  return constructor.newInstance(
                                                      declaringClass, MethodHandles.Lookup.PRIVATE)
                                                      .unreflectSpecial(m, declaringClass)
                                                      .bindTo(proxy)
                                                      .invokeWithArguments(args);
                                              }
                                          }
                                      });
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

    @Override
    public void close() throws Exception {
        classloader.close();
    }
}
