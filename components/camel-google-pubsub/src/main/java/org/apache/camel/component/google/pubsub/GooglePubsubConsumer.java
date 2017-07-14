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
import java.util.concurrent.ExecutorService;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriptionName;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.google.pubsub.consumer.ExchangeAckTransaction;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.spi.Synchronization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GooglePubsubConsumer extends DefaultConsumer {

    private Logger localLog;

    private final Processor processor;
    private final Synchronization ackStrategy;
    private SubscriberGrpc.SubscriberStub subscriberStub;


    GooglePubsubConsumer(GooglePubsubEndpoint endpoint, Processor processor) throws Exception {
        super(endpoint, processor);
        this.processor = processor;
        this.ackStrategy = new ExchangeAckTransaction(getEndpoint());

        String loggerId = endpoint.getLoggerId();

        if (Strings.isNullOrEmpty(loggerId)) {
            loggerId = this.getClass().getName();
        }

        localLog = LoggerFactory.getLogger(loggerId);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        localLog.info("Starting Google PubSub consumer for {}/{}", getEndpoint().getProjectId(), getEndpoint().getDestinationName());

        ManagedChannel channel;
        try {
            channel = getEndpoint().getChannelProvider().getChannel();
        } catch (Exception e) {
            localLog.error("Failure getting channel from PubSub : ", e);
            return;
        }

        subscriberStub = SubscriberGrpc.newStub(channel);

        for (int i = 0; i < getEndpoint().getConcurrentConsumers(); i++) {
            doPoll();
        }
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        localLog.info("Stopping Google PubSub consumer for {}/{}", getEndpoint().getProjectId(), getEndpoint().getDestinationName());
    }

    private void doPoll() {
        if (!isRunAllowed() || isSuspendingOrSuspended()) {
            return;
        }

        SubscriptionName subscriptionFullName = SubscriptionName.create(
                getEndpoint().getProjectId(),
                getEndpoint().getDestinationName()
        );

        PullRequest pullRequest = PullRequest.newBuilder()
                .setSubscriptionWithSubscriptionName(subscriptionFullName)
                .setMaxMessages(getEndpoint().getMaxMessagesPerPoll())
                .build();


        ManagedChannel channel;
        try {
            channel = getEndpoint().getChannelProvider().getChannel();
        } catch (Exception e) {
            localLog.error("Failure getting channel from PubSub : ", e);
            return;
        }

        subscriberStub
                .pull(pullRequest, new StreamObserver<PullResponse>() {
                    @Override
                    public void onNext(PullResponse value) {
                        for (ReceivedMessage receivedMessage : value.getReceivedMessagesList()) {
                            PubsubMessage pubsubMessage = receivedMessage.getMessage();

                            byte[] body = pubsubMessage.getData().toByteArray();

                            if (localLog.isTraceEnabled()) {
                                localLog.trace("Received message ID : {}", pubsubMessage.getMessageId());
                            }

                            Exchange exchange = getEndpoint().createExchange();
                            exchange.getIn().setBody(body);

                            exchange.getIn().setHeader(GooglePubsubConstants.ACK_ID, receivedMessage.getAckId());
                            exchange.getIn().setHeader(GooglePubsubConstants.MESSAGE_ID, pubsubMessage.getMessageId());
                            exchange.getIn().setHeader(GooglePubsubConstants.PUBLISH_TIME, pubsubMessage.getPublishTime());

                            if (null != receivedMessage.getMessage().getAttributesMap()) {
                                exchange.getIn().setHeader(GooglePubsubConstants.ATTRIBUTES, receivedMessage.getMessage().getAttributesMap());
                            }

                            if (getEndpoint().getAckMode() != GooglePubsubConstants.AckMode.NONE) {
                                exchange.addOnCompletion(ackStrategy);
                            }

                            try {
                                processor.process(exchange);
                            } catch (Throwable e) {
                                exchange.setException(e);
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        localLog.error("Failure while retrieving messages from PubSub : ", t);
                    }

                    @Override
                    public void onCompleted() {
                        localLog.trace("Completed retrieving messages - poll again");
                        doPoll();
                    }
                });
    }


    @Override
    public GooglePubsubEndpoint getEndpoint() {
        return (GooglePubsubEndpoint) super.getEndpoint();
    }
}

