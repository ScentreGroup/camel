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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

import com.google.api.client.util.Strings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.ApiException;
import com.google.api.gax.grpc.ChannelProvider;
import com.google.api.gax.grpc.ExecutorProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.spi.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.spi.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.spi.v1.TopicAdminClient;
import com.google.cloud.pubsub.spi.v1.TopicAdminSettings;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import org.apache.camel.CamelContext;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubsubTestSupport extends CamelTestSupport {

    public static final String CHANNEL_ENDPOINT;
    public static final Boolean CHANNEL_IS_PLAIN_TEXT;
    public static final String CREDENTIALS_JSON_FILE;

    public static final String SERVICE_KEY;
    public static final String SERVICE_ACCOUNT;
    public static final String PROJECT_ID;
    public static final String SERVICE_HOST;
    public static final int SERVICE_PORT;

    private static final Logger LOG = LoggerFactory.getLogger(PubsubTestSupport.class);

    static {
        Properties testProperties = loadProperties();
        CHANNEL_ENDPOINT = testProperties.getProperty("test.channel.endpoint");
        CHANNEL_IS_PLAIN_TEXT = Boolean.valueOf(testProperties.getProperty("test.channel.plainText"));
        CREDENTIALS_JSON_FILE = testProperties.getProperty("test.credentials.file");


        SERVICE_KEY = testProperties.getProperty("service.key");
        SERVICE_ACCOUNT = testProperties.getProperty("service.account");
        PROJECT_ID = testProperties.getProperty("project.id");
        SERVICE_HOST = testProperties.getProperty("test.channel.host");
        SERVICE_PORT = Integer.parseInt(testProperties.getProperty("test.channel.port", "8085"));
    }

    private static ChannelProvider createChannelProvider(ExecutorProvider executorProvider) {
        ChannelProvider channelProvider;
        if (CHANNEL_IS_PLAIN_TEXT) {
            channelProvider = PlainTextChannelProvider.create(CHANNEL_ENDPOINT);
        } else {
            Credentials credentials;

            if (!Strings.isNullOrEmpty(CREDENTIALS_JSON_FILE)) {
                try {
                    credentials = GoogleCredentials.fromStream(new FileInputStream(CREDENTIALS_JSON_FILE));
                } catch (IOException e) {
                    String error = "Failed to load credentials from file: " + e.getMessage();
                    LOG.error(error, e);
                    throw new RuntimeException(e);
                }
            } else {
                try {
                    credentials = GoogleCredentials.getApplicationDefault();
                } catch (IOException e) {
                    String error = "Failed to load application default credentials: " + e.getMessage();
                    LOG.error(error, e);
                    throw new RuntimeException(e);
                }
            }

            channelProvider = TopicAdminSettings
                    .defaultChannelProviderBuilder()
                    .setExecutorProvider(
                            executorProvider == null ? TopicAdminSettings.defaultExecutorProviderBuilder().build() : executorProvider
                    )
                    .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                    .setEndpoint(Strings.isNullOrEmpty(CHANNEL_ENDPOINT) ? TopicAdminSettings.getDefaultEndpoint() : CHANNEL_ENDPOINT)
                    .build();
        }
        return channelProvider;
    }


    private static Properties loadProperties() {
        Properties testProperties = new Properties();
        InputStream fileIn = PubsubTestSupport.class.getClassLoader().getResourceAsStream("simple.properties");
        try {
            testProperties.load(fileIn);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return testProperties;
    }

    protected void addPubsubComponent(CamelContext context) throws Exception {
        CredentialsProvider credentialsProvider = new GooglePubsubCredentialsProviderBuilder()
                    .setServiceAccount(SERVICE_ACCOUNT)
                     .setServiceAccountKey(SERVICE_KEY)
                .build();

        GooglePubsubComponent component = new GooglePubsubComponent();
        component.setChannelProvider(createChannelProvider(new ExecutorProvider() {
            @Override
            public boolean shouldAutoClose() {
                return true;
            }

            @Override
            public ScheduledExecutorService getExecutor() {
                return context.getExecutorServiceManager().newDefaultScheduledThreadPool(this, "channel-provider");
            }
        }));
        component.setCredentialsProvider(credentialsProvider);

        context.addComponent("google-pubsub", component);
        context.addComponent("properties", new PropertiesComponent("ref:prop"));
    }

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry jndi = super.createRegistry();
        jndi.bind("prop", loadProperties());
        return jndi;
    }

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();
        addPubsubComponent(context);
        return context;
    }

    public static void createTopicSubscriptionPair(String topicId, String subscriptionId) throws Exception {
        createTopicSubscriptionPair(topicId, subscriptionId, 10);
    }

    public static void createTopicSubscriptionPair(String topicId, String subscriptionId, int ackDeadlineSeconds) throws Exception {
        ChannelProvider channelProvider = createChannelProvider(null);
        try (
                SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(
                        SubscriptionAdminSettings.defaultBuilder()
                                .setChannelProvider(channelProvider)
                                .build()
                );
                TopicAdminClient topicAdminClient = TopicAdminClient.create(
                        TopicAdminSettings.defaultBuilder()
                                .setChannelProvider(channelProvider)
                                .build()
                )
        ) {
            TopicName topicName = TopicName.create(PubsubTestSupport.PROJECT_ID, topicId);
            try {
                topicAdminClient.createTopic(topicName);
            } catch (ApiException e) {
                if (io.grpc.Status.Code.ALREADY_EXISTS != e.getStatusCode()) {
                    throw e;
                }
            }
            SubscriptionName subscriptionName =
                    SubscriptionName.create(PubsubTestSupport.PROJECT_ID, subscriptionId);
            // create a pull subscription with default acknowledgement deadline

            try {
                subscriptionAdminClient.createSubscription(
                        subscriptionName, topicName, PushConfig.getDefaultInstance(), ackDeadlineSeconds);
            } catch (ApiException e) {
                if (io.grpc.Status.Code.ALREADY_EXISTS != e.getStatusCode()) {
                    throw e;
                }
            }
        }
    }

}
