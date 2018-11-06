/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.util;


import brave.Tracing;

import brave.opentracing.BraveTracer;

import brave.sampler.CountingSampler;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

import io.opentracing.Scope;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.util.GlobalTracer;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.conf.ComponentConfiguration;
import org.apache.commons.configuration.Configuration;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Sender;
import zipkin2.reporter.urlconnection.URLConnectionSender;

/**
 * Utilities for tracing.
 */
@Slf4j
public class Traces {

    public static final String ZIPKIN_REPORTER = "zipkinReporter";
    public static final String ZIPKIN_REPORTER_DEFAULT = "NOOP";

    public static final String ZIPKIN_SERVICE_NAME = "zipkinServiceName";
    public static final String ZIPKIN_SERVICE_NAME_DEFAULT = "BK";

    public static final String ZIPKIN_URL = "zipkinUrl";
    public static final String ZIPKIN_URL_DEFAULT = "http://localhost:9411/api/v2/spans";

    // rate 0 means never sample, 1 means always sample.
    // Otherwise minimum sample rate is 0.01, or 1% of traces
    public static final String ZIPKIN_SAMPLE_RATE = "zipkinSampleRate";
    public static final float ZIPKIN_SAMPLE_RATE_DEFAULT = 0.0f;

    private static final ConcurrentLinkedQueue<Closeable> toClose = new ConcurrentLinkedQueue<>();

    public static AbstractLifecycleComponent<ComponentConfiguration> asLifecycleCompenent(final Configuration config) {
        return new AbstractLifecycleComponent<ComponentConfiguration>("tracer", null, null) {
            @Override
            protected void doStart() {
                Traces.createAndRegisterTracer(config);
            }

            @Override
            protected void doStop() {
                // noop
            }

            @Override
            protected void doClose() throws IOException {
                Traces.closeTracer();
            }
        };
    }

    // do this once and use GlobalTracer.get() as needed.
    public static Tracer createAndRegisterTracer(Configuration conf) {
        if (GlobalTracer.isRegistered()) {
            log.info("Tracer already registered");
            return GlobalTracer.get();
        }

        if (conf == null) {
            log.info("Cannot register tracer. Invalid configuration: null");
            return GlobalTracer.get();
        }

        final String zipkinReporter = conf.getString(ZIPKIN_REPORTER, ZIPKIN_REPORTER_DEFAULT).toUpperCase();
        final String serviceName = conf.getString(ZIPKIN_SERVICE_NAME, ZIPKIN_SERVICE_NAME_DEFAULT);
        float sampleRate = conf.getFloat(ZIPKIN_SAMPLE_RATE, ZIPKIN_SAMPLE_RATE_DEFAULT);
        sampleRate = sampleRate < 0.0f ? 0.0f : sampleRate;
        sampleRate = sampleRate > 1.0f ? 1.0f : sampleRate;

        final Tracer tracer;
        switch (zipkinReporter) {
            case "CONSOLE":
                final Tracing braveTracing = Tracing.newBuilder()
                        .localServiceName(serviceName)
                        .spanReporter(AsyncReporter.CONSOLE)
                        .sampler(CountingSampler.create(sampleRate))
                        .build();
                tracer = BraveTracer.create(braveTracing);
                toClose.add(braveTracing);
                log.info("configured tracer");
                break;
            case "URL":
                final String zipkinUrl = conf.getString(ZIPKIN_URL, ZIPKIN_URL_DEFAULT);
                final Sender sender = URLConnectionSender.create(zipkinUrl);
                final AsyncReporter<Span> reporter = AsyncReporter.create(sender);
                toClose.add(reporter);
                toClose.add(sender);

                final Tracing braveTracing2 = Tracing.newBuilder()
                        .localServiceName(serviceName)
                        .spanReporter(reporter)
                        .sampler(CountingSampler.create(sampleRate))
                        .build();
                tracer = BraveTracer.create(braveTracing2);
                toClose.add(braveTracing2);
                log.info("configured tracer");
                break;
            case "NOOP":
                // NOOP is a default tracer
                log.info("Tracing is disabled");
                return GlobalTracer.get();
            default:
                log.warn("Unknown tracer {}", zipkinReporter);
                throw new IllegalArgumentException("Unknown tracer: " + zipkinReporter);
        }

        try {
            GlobalTracer.register(tracer);
            log.debug("Successfully resolved a Tracer instance and registered it as the global Tracer");
        } catch (Exception e) {
            log.warn("Could not register Tracer through GlobalTracer", e);
        }
        return GlobalTracer.get();
    }

    public static void safeClose(io.opentracing.Span span) {
        if (span != null) {
            span.finish();
        }
    }

    public static void safeClose(Scope scope) {
        if (scope != null) {
            scope.close();
        }
    }

    // This will not allow register another tracer (in i.e. unit tests)
    // with exclusion of NOOP -> any other tracer.
    // if need arises to close and change tracer dynamically (doubt that)
    // we'll have to create a Tracer wrapper allowing that and register the wrapper.
    // It will close async reporter to flush pending traces out.
    public static void closeTracer() {
        Closeable c;
        while ((c = toClose.poll()) != null) {
            try {
                c.close();
            } catch (Throwable t) {
                log.warn("Error while closing tracer", t);
            }
        }
    }

    public static SpanContext byteStringToSpanContext(ByteString byteString) {
        if (byteString == null || byteString.isEmpty()) {
            return null;
        }
        SpanContext context = null;
        try {
            try (ObjectInputStream objStream = new ObjectInputStream(byteString.newInput())) {
                @SuppressWarnings("unchecked")
                Map<String, String> carrier = (Map<String, String>) objStream.readObject();
                context = GlobalTracer.get()
                        .extract(Format.Builtin.TEXT_MAP, new TextMapExtractAdapter(carrier));
            }
        } catch (Exception e) {
            log.warn("Could not deserialize context", e);
        }
        return context;
    }

    public static ByteString spanContextToByteString(SpanContext context) {
        if (context == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        GlobalTracer.get().inject(context, Format.Builtin.TEXT_MAP, new TextMapInjectAdapter(carrier));
        if (carrier.isEmpty()) {
            return null;
        }
        ByteString byteString = null;
        try {
            try (ByteArrayOutputStream stream = new ByteArrayOutputStream();
                 ObjectOutputStream objStream = new ObjectOutputStream(stream)) {
                objStream.writeObject(carrier);
                objStream.flush();
                byteString = UnsafeByteOperations.unsafeWrap(stream.toByteArray());
            }
        } catch (IOException e) {
            log.warn("Could not serialize context", e);
        }
        return byteString;
    }

}
