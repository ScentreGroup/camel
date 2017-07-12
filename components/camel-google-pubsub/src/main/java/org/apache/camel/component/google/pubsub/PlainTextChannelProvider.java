/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.google.pubsub;

import java.io.IOException;
import java.util.concurrent.Executor;

import com.google.api.gax.grpc.ChannelProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Plaintext channel provider required for connecting to Pubsub emulator
 *
 */
public final class PlainTextChannelProvider implements ChannelProvider {
    private final String endpoint;

    private PlainTextChannelProvider(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public boolean shouldAutoClose() {
        return false;
    }

    @Override
    public boolean needsExecutor() {
        return false;
    }

    @Override
    public ManagedChannel getChannel() throws IOException {
        return ManagedChannelBuilder.forTarget(endpoint).usePlaintext(true).build();
    }

    @Override
    public ManagedChannel getChannel(Executor executor) throws IOException {
        return getChannel();
    }

    public static PlainTextChannelProvider create(String endpoint) {
        return new PlainTextChannelProvider(endpoint);
    }
}
