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
package org.apache.camel.component.google.pubsub.integration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.google.pubsub.PubsubTestSupport;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultExchange;
import org.junit.BeforeClass;
import org.junit.Test;

public class FailureTest extends PubsubTestSupport {

    private static final String TOPIC_NAME = "failureTest";
    private static final String SUBSCRIPTION_NAME = "failureTestSub";

    @EndpointInject(uri = "direct:from")
    private Endpoint directIn;

    @EndpointInject(uri = "google-pubsub:{{project.id}}:" + TOPIC_NAME + "_DOES_NOT_EXIST")
    private Endpoint pubsubTopicDoesNotExist;

    @EndpointInject(uri = "mock:sendResult")
    private MockEndpoint sendResult;

    @EndpointInject(uri = "mock:receiveException")
    private MockEndpoint receiveException;

    @Produce(uri = "direct:from")
    private ProducerTemplate producer;

    @BeforeClass
    public static void createTopicSubscription() throws Exception {
       // createTopicSubscriptionPair(TOPIC_NAME, SUBSCRIPTION_NAME);
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() {
                onException()
                        .handled(true)
                        .log("Exception caught ${exception}")
                        .to(receiveException);

                from(directIn)
                        .routeId("TopicDoesNotExist")
                        .to(pubsubTopicDoesNotExist)
                        .to(sendResult);
            }
        };
    }

    /**
     * Expected exception handler to be invoked.
     *
     * Sending to a non existant topic which will result in an error and an exception being
     * thrown which should be caught by the onException handler.
     */
    @Test
    public void sendToNonExistantTopic() throws Exception {

        Exchange exchange = new DefaultExchange(context);

        receiveException.expectedMessageCount(1);
        sendResult.expectedMessageCount(0);

        producer.send(exchange);

        receiveException.assertIsSatisfied(1000);
        sendResult.assertIsSatisfied(1000);
    }
}
