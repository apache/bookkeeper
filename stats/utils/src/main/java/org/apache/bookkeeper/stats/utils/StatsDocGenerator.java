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

package org.apache.bookkeeper.stats.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.Stats;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;
import org.yaml.snakeyaml.DumperOptions.ScalarStyle;
import org.yaml.snakeyaml.Yaml;

/**
 * Generator stats documentation.
 */
@Slf4j
public class StatsDocGenerator {

    enum StatsType {
        COUNTER,
        GAUGE,
        OPSTATS
    }

    @AllArgsConstructor
    @Data
    static class StatsDocEntry {
        private String name;
        private StatsType type;
        private String description;

        public Map<String, String> properties() {
            Map<String, String> properties = new TreeMap<>();
            properties.put("type", type.name());
            properties.put("description", description);
            return properties;
        }
    }

    private static Reflections newReflections(String packagePrefix) {
        List<URL> urls = new ArrayList<>();
        ClassLoader[] classLoaders = new ClassLoader[] {
            StatsDocGenerator.class.getClassLoader(),
            Thread.currentThread().getContextClassLoader()
        };
        for (int i = 0; i < classLoaders.length; i++) {
            if (classLoaders[i] instanceof URLClassLoader) {
                urls.addAll(Arrays.asList(((URLClassLoader) classLoaders[i]).getURLs()));
            } else {
                throw new RuntimeException("ClassLoader '" + classLoaders[i] + " is not an instance of URLClassLoader");
            }
        }
        Predicate<String> filters = new FilterBuilder()
            .includePackage(packagePrefix);
        ConfigurationBuilder confBuilder = new ConfigurationBuilder();
        confBuilder.filterInputsBy(filters);
        confBuilder.setUrls(urls);
        return new Reflections(confBuilder);
    }

    private final String packagePrefix;
    private final Reflections reflections;
    private final StatsProvider statsProvider;
    private final NavigableMap<String, NavigableMap<String, StatsDocEntry>> docEntries = new TreeMap<>();

    public StatsDocGenerator(String packagePrefix,
                             StatsProvider provider) {
        this.packagePrefix = packagePrefix;
        this.reflections = newReflections(packagePrefix);
        this.statsProvider = provider;
    }

    public void generate(String filename) throws Exception {
        log.info("Processing classes under package {}", packagePrefix);
        // get all classes annotated with `StatsDoc`
        Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(StatsDoc.class);
        log.info("Retrieve all `StatsDoc` annotated classes : {}", annotatedClasses);

        for (Class<?> annotatedClass : annotatedClasses) {
            generateDocForAnnotatedClass(annotatedClass);
        }
        log.info("Successfully processed classes under package {}", packagePrefix);
        log.info("Writing stats doc to file {}", filename);
        writeDoc(filename);
        log.info("Successfully wrote stats doc to file {}", filename);
    }

    private void generateDocForAnnotatedClass(Class<?> annotatedClass) {
        StatsDoc scopeStatsDoc = annotatedClass.getDeclaredAnnotation(StatsDoc.class);
        if (scopeStatsDoc == null) {
            return;
        }

        log.info("Processing StatsDoc annotated class {} : {}", annotatedClass, scopeStatsDoc);

        Field[] fields = annotatedClass.getDeclaredFields();
        for (Field field : fields) {
            StatsDoc fieldStatsDoc = field.getDeclaredAnnotation(StatsDoc.class);
            if (null == fieldStatsDoc) {
                // it is not a `StatsDoc` annotated field
                continue;
            }
            generateDocForAnnotatedField(scopeStatsDoc, fieldStatsDoc, field);
        }

        log.info("Successfully processed StatsDoc annotated class {}.", annotatedClass);
    }

    private NavigableMap<String, StatsDocEntry> getCategoryMap(String category) {
        NavigableMap<String, StatsDocEntry> categoryMap = docEntries.get(category);
        if (null == categoryMap) {
            categoryMap = new TreeMap<>();
            docEntries.put(category, categoryMap);
        }
        return categoryMap;
    }

    private void generateDocForAnnotatedField(StatsDoc scopedStatsDoc, StatsDoc fieldStatsDoc, Field field) {
        NavigableMap<String, StatsDocEntry> categoryMap = getCategoryMap(scopedStatsDoc.category());

        String statsName =
            statsProvider.getStatsName(scopedStatsDoc.scope(), scopedStatsDoc.name(), fieldStatsDoc.name());
        StatsType statsType;
        if (Counter.class.isAssignableFrom(field.getType())) {
            statsType = StatsType.COUNTER;
        } else if (Gauge.class.isAssignableFrom(field.getType())) {
            statsType = StatsType.GAUGE;
        } else if (OpStatsLogger.class.isAssignableFrom(field.getType())) {
            statsType = StatsType.OPSTATS;
        } else {
            throw new IllegalArgumentException("Unknown stats field '" + field.getName()
                + "' is annotated with `StatsDoc`: " + field.getType());
        }

        String helpDesc = fieldStatsDoc.help();
        StatsDocEntry docEntry = new StatsDocEntry(statsName, statsType, helpDesc);
        categoryMap.put(statsName, docEntry);
    }

    private void writeDoc(String file) throws IOException {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(FlowStyle.BLOCK);
        options.setDefaultScalarStyle(ScalarStyle.LITERAL);
        Yaml yaml = new Yaml(options);
        Writer writer;
        if (Strings.isNullOrEmpty(file)) {
            writer = new OutputStreamWriter(System.out, UTF_8);
        } else {
            writer = new OutputStreamWriter(new FileOutputStream(file), UTF_8);
        }
        try {
            Map<String, Map<String, Map<String, String>>> docs = docEntries.entrySet()
                .stream()
                .collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> e.getValue().entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                            e1 -> e1.getKey(),
                            e1 -> e1.getValue().properties()
                        ))
                ));
            yaml.dump(docs, writer);
            writer.flush();
        } finally {
            writer.close();
        }
    }

    /**
     * Args for stats generator.
     */
    private static class MainArgs {

        @Parameter(
            names = {
                "-p", "--package"
            },
            description = "Package prefix of the classes to generate stats doc")
        String packagePrefix = "org.apache.bookkeeper";

        @Parameter(
            names = {
                "-sp", "--stats-provider"
            },
            description = "The stats provider used for generating stats doc")
        String statsProviderClass = "prometheus";

        @Parameter(
            names = {
                "-o", "--output-yaml-file"
            },
            description = "The output yaml file to dump stats docs."
                + " If omitted, the output goes to stdout."
        )
        String yamlFile = null;

        @Parameter(
            names = {
                "-h", "--help"
            },
            description = "Show this help message")
        boolean help = false;

    }

    public static void main(String[] args) throws Exception {
        MainArgs mainArgs = new MainArgs();

        JCommander commander = new JCommander();
        try {
            commander.setProgramName("stats-doc-gen");
            commander.addObject(mainArgs);
            commander.parse(args);
            if (mainArgs.help) {
                commander.usage();
                Runtime.getRuntime().exit(0);
                return;
            }
        } catch (Exception e) {
            commander.usage();
            Runtime.getRuntime().exit(-1);
            return;
        }

        Stats.loadStatsProvider(getStatsProviderClass(mainArgs.statsProviderClass));
        StatsProvider provider = Stats.get();

        StatsDocGenerator docGen = new StatsDocGenerator(
            mainArgs.packagePrefix,
            provider
        );
        docGen.generate(mainArgs.yamlFile);
    }

    private static String getStatsProviderClass(String providerClass) {
        switch (providerClass.toLowerCase()) {
            case "prometheus":
                return "org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider";
            case "codahale":
                return "org.apache.bookkeeper.stats.codahale.CodahaleMetricsProvider";
            default:
                return providerClass;
        }
    }

}
