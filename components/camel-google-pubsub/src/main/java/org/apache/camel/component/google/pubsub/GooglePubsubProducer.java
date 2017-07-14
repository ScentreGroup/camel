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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.api.client.util.Strings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.grpc.stub.StreamObserver;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultAsyncProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic PubSub Producer
 */
public class GooglePubsubProducer extends DefaultAsyncProducer {

    private final Logger logger;
    private final PublisherGrpc.PublisherStub publisherStub;

    public GooglePubsubProducer(GooglePubsubEndpoint endpoint) throws Exception {
        super(endpoint);

        String loggerId = endpoint.getLoggerId();

        if (Strings.isNullOrEmpty(loggerId)) {
            loggerId = this.getClass().getName();
        }

        logger = LoggerFactory.getLogger(loggerId);

        publisherStub = PublisherGrpc.newStub(
                getEndpoint().getChannelProvider().getChannel()
        );

    }

    /**
     * The incoming message is expected to be either
     * - a List of Exchanges (aggregated)
     * - an Exchange
     */
    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        List<Exchange> entryList = prepareExchangeList(exchange);

        if (entryList == null || entryList.size() == 0) {
            logger.warn("The incoming message is either null or empty. Triggered by an aggregation timeout?");
            callback.done(true);
            return true;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("uploader thread/id: "
                    + Thread.currentThread().getId()
                    + " / " + exchange.getExchangeId()
                    + " . api call completed.");
        }

        try {
            sendMessages(exchange, callback);
        } catch (Exception e) {
            exchange.setException(e);
            callback.done(true);
            return true;
        }
        return false;
    }

    /**
     * The method converts a single incoming message into a List
     *
     * @param exchange
     * @return
     */
    private static List<Exchange> prepareExchangeList(Exchange exchange) {

        List<Exchange> entryList = null;

        if (null == exchange.getProperty(Exchange.GROUPED_EXCHANGE)) {
            entryList = new ArrayList<>();
            entryList.add(exchange);
        } else {
            entryList = (List<Exchange>) exchange.getProperty(Exchange.GROUPED_EXCHANGE);
        }

        return entryList;
    }

    private void sendMessages(Exchange exchange, AsyncCallback callback) throws Exception {
        List<Exchange> exchanges = prepareExchangeList(exchange);

        GooglePubsubEndpoint endpoint = getEndpoint();
        TopicName topicName = TopicName.create(endpoint.getProjectId(), endpoint.getDestinationName());

        List<PubsubMessage> messages = new ArrayList<>();

        PublishRequest.Builder requestBuilder = PublishRequest.newBuilder();

        for (Exchange ex : exchanges) {
            PubsubMessage.Builder messageBuilder = PubsubMessage.newBuilder();

            Object body = ex.getIn().getBody();

            if (body instanceof String) {
                messageBuilder.setData(ByteString.copyFromUtf8((String) body));
            } else if (body instanceof byte[]) {
                messageBuilder.setData(ByteString.copyFrom((byte[]) body));
            } else {
                messageBuilder.setData(ByteString.copyFrom(serialize(body)));
            }

            Object attributes = ex.getIn().getHeader(GooglePubsubConstants.ATTRIBUTES);

            if (attributes != null && attributes instanceof Map && ((Map) attributes).size() > 0) {
                messageBuilder.putAllAttributes((Map) attributes);
            }
            requestBuilder.addMessages(messageBuilder.build());
        }

        requestBuilder.setTopicWithTopicName(topicName);

        publisherStub.publish(requestBuilder.build(), new StreamObserver<PublishResponse>() {
            private List<String> messageIds = new ArrayList<>(exchanges.size());

            @Override
            public void onNext(PublishResponse value) {
                messageIds.addAll(value.getMessageIdsList());

            }

            @Override
            public void onError(Throwable t) {
                exchange.setException(t);
                exchanges.forEach(ex -> ex.setException(t));
                callback.done(false);
            }

            @Override
            public void onCompleted() {
                Iterator<Exchange> exchangeIterator = exchanges.iterator();
                Iterator<String> messageIdIterator = messageIds.iterator();
                while (exchangeIterator.hasNext() && messageIdIterator.hasNext()) {
                    exchangeIterator.next().getIn().setHeader(GooglePubsubConstants.MESSAGE_ID, messageIdIterator.next());
                }
                callback.done(false);
            }
        });
    }

    @Override
    public GooglePubsubEndpoint getEndpoint() {
        return (GooglePubsubEndpoint) super.getEndpoint();
    }

    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }
}
