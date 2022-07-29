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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import groovy.lang.Closure;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.jboss.shrinkwrap.resolver.api.maven.ConfigurableMavenResolverSystem;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.ScopeType;
import org.jboss.shrinkwrap.resolver.api.maven.coordinate.MavenDependencies;
import org.jboss.shrinkwrap.resolver.api.maven.coordinate.MavenDependency;

/**
 * A maven class loader for resolving and loading maven artifacts.
 */
public class MavenClassLoader implements AutoCloseable {

    private static List<File> currentVersionLibs;

    private MavenClassLoader(ClassLoader cl) {
        this.classloader = cl;
    }

    private final ClassLoader classloader;

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
                        && a.getCoordinate().getArtifactId().equals("slf4j-1.2-api"))
                .map((a) -> a.getCoordinate().getVersion())
                .findFirst();

        List<MavenDependency> deps = Lists.newArrayList(
                MavenDependencies.createDependency(
                        mainArtifact, ScopeType.COMPILE, false));
        if (slf4jVersion.isPresent()) {
            deps.add(MavenDependencies.createDependency("org.slf4j:slf4j-simple:" + slf4jVersion.get(),
                    ScopeType.COMPILE, false));
        }

        File[] files = resolver.addDependencies(deps.toArray(new MavenDependency[0]))
                .resolve().withTransitivity().asFile();
        return createClassLoader(files);
    }

    private static MavenClassLoader createClassLoader(File[] jars) {
        final ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
        URLClassLoader cl = new URLClassLoader(Arrays.stream(jars)
                .map((f) -> {
                    try {
                        return f.toURI().toURL();
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                })
                .toArray(URL[]::new),
                systemClassLoader) {

            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                Class<?> loadedClass = findLoadedClass(name);
                if (loadedClass == null) {
                    try {
                        loadedClass = findClass(name);
                    } catch (ClassNotFoundException ignored) {
                    }
                    if (loadedClass == null) {
                        try {
                            loadedClass = systemClassLoader.loadClass(name);
                        } catch (ClassNotFoundException e) {
                        }
                    }
                }
                if (resolve && loadedClass != null) {
                    resolveClass(loadedClass);
                }
                return loadedClass;
            }
        };
        return new MavenClassLoader(cl);
    }

    public static MavenClassLoader forBookKeeperVersion(String version) throws Exception {
        if (version.equals(BookKeeperClusterUtils.CURRENT_VERSION)) {
            return forBookkeeperCurrentVersion();
        }
        return forArtifact("org.apache.bookkeeper:bookkeeper-server:" + version);
    }

    private static MavenClassLoader forBookkeeperCurrentVersion() throws Exception {
        if (currentVersionLibs == null) {
            final String version = BookKeeperClusterUtils.CURRENT_VERSION;
            final String artifactName = "bookkeeper-server-" + version + "-bin";
            final Path tarFile = Paths.get("..", "..", "..",
                    "bookkeeper-dist", "server", "build", "distributions", artifactName + ".tar.gz");
            final File tempDir = new File("build");
            extractTarGz(tarFile.toFile(), tempDir);
            List<File> jars = new ArrayList<>();
            Files.list(Paths.get(tempDir.getAbsolutePath(), "bookkeeper-server-" + version, "lib"))
                    .forEach(path -> {
                        jars.add(path.toFile());
                    });
            currentVersionLibs = jars;
        }
        return createClassLoader(currentVersionLibs.toArray(new File[]{}));
    }

    public Object callStaticMethod(String className, String methodName, ArrayList<?> args) throws Exception {
        Class<?> klass = Class.forName(className, true, classloader);

        try {
            Class<?>[] paramTypes = args.stream().map((a) -> a.getClass()).toArray(Class[]::new);
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
        return klass
                .getConstructor(Arrays.stream(args).map((a) ->
                                a.getClass())
                        .toArray(Class[]::new))
                .newInstance(args);
    }

    public Object newBookKeeper(String zookeeper) throws Exception {
        Class<?> clientConfigurationClass = Class
                .forName("org.apache.bookkeeper.conf.ClientConfiguration", true, classloader);
        Object clientConfiguration = newInstance("org.apache.bookkeeper.conf.ClientConfiguration");
        clientConfigurationClass
                .getMethod("setZkServers", String.class)
                .invoke(clientConfiguration, zookeeper);

        // relax timeouts in order to get tests passing in limited environments
        clientConfigurationClass
                .getMethod("setReadTimeout", int.class)
                .invoke(clientConfiguration, 15);

        clientConfigurationClass
                .getMethod("setZkTimeout", int.class)
                .invoke(clientConfiguration, 30_000);
        Class<?> klass = Class.forName("org.apache.bookkeeper.client.BookKeeper", true, classloader);
        return klass
                .getConstructor(clientConfigurationClass)
                .newInstance(clientConfiguration);

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
        if (classloader instanceof Closeable) {
            ((Closeable) classloader).close();
        }
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private static void extractTarGz(File tarGz, File output) throws Exception {
        File tarFile = new File(output, tarGz.getName().replace(".gz", ""));
        tarFile.delete();
        deCompressGZipFile(tarGz, tarFile);
        unTar(tarFile, output);
    }

    private static File deCompressGZipFile(File gZippedFile, File tarFile) throws IOException {
        try (GZIPInputStream gZIPInputStream = new GZIPInputStream(new FileInputStream(gZippedFile));
             FileOutputStream fos = new FileOutputStream(tarFile)) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gZIPInputStream.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }
        }
        return tarFile;
    }

    @SuppressFBWarnings({"RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE"})
    private static void unTar(final File inputFile, final File outputDir) throws Exception {
        try (final FileInputStream fis = new FileInputStream(inputFile);
                TarArchiveInputStream debInputStream = (TarArchiveInputStream)
                new ArchiveStreamFactory().createArchiveInputStream("tar", fis)) {
            TarArchiveEntry entry;
            while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
                final File outputFile = new File(outputDir, entry.getName());
                if (entry.isDirectory()) {
                    if (!outputFile.exists()) {
                        if (!outputFile.mkdirs()) {
                            throw new IllegalStateException(
                                    String.format("Couldn't create directory %s.", outputFile.getAbsolutePath()));
                        }
                    } else {
                        outputFile.delete();
                    }
                } else {
                    try (final OutputStream outputFileStream = new FileOutputStream(outputFile)) {
                        IOUtils.copy(debInputStream, outputFileStream);
                    }
                }
            }
        }
    }


}
