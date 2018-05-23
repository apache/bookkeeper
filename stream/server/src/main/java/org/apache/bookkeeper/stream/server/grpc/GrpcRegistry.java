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

package org.apache.bookkeeper.stream.server.grpc;

import com.google.common.io.ByteStreams;
import io.grpc.HandlerRegistry;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.ServerCallHandler;
import io.grpc.ServerMethodDefinition;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;

/**
 * Registry for grpc services.
 */
public class GrpcRegistry extends HandlerRegistry {

    private static class ByteMarshaller implements MethodDescriptor.Marshaller<byte[]> {

        @Override
        public InputStream stream(byte[] value) {
            return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(InputStream stream) {
            try {
                return ByteStreams.toByteArray(stream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private final MethodDescriptor.Marshaller<byte[]> byteMarshaller = new ByteMarshaller();
    private final ServerCallHandler<byte[], byte[]> handler;

    public GrpcRegistry(ServerCallHandler<byte[], byte[]> handler) {
        this.handler = handler;
    }

    @Nullable
    @Override
    public ServerMethodDefinition<?, ?> lookupMethod(String methodName,
                                                     @Nullable String authority) {
        MethodDescriptor<byte[], byte[]> methodDescriptor = MethodDescriptor.newBuilder(byteMarshaller, byteMarshaller)
            .setFullMethodName(methodName)
            .setType(MethodType.UNKNOWN)
            .build();
        return ServerMethodDefinition.create(methodDescriptor, handler);
    }
}
