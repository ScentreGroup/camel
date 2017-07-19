package org.apache.camel.example.google.pubsub;

import java.util.Properties;

import com.google.api.gax.grpc.ApiException;
import com.google.api.gax.grpc.ChannelProvider;
import com.google.cloud.pubsub.spi.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.spi.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.spi.v1.TopicAdminClient;
import com.google.cloud.pubsub.spi.v1.TopicAdminSettings;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import org.apache.camel.component.google.pubsub.GooglePubsubConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTopicSubscription {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTopicSubscription.class);

    public static void main(String[] args) throws Exception{
        createTopicSubscriptionPair(10);
    }


    public static void createTopicSubscriptionPair(int ackDeadlineSeconds) throws Exception {
        GooglePubsubConnectionFactory connectionFactory = PubsubUtil.createConnectionFactory();
        Properties properties = PubsubUtil.loadProperties();
        ChannelProvider channelProvider = connectionFactory
                .getChannelProvider(SubscriptionAdminSettings.defaultExecutorProviderBuilder().build());

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
            String projectId = properties.getProperty("pubsub.projectId");
            String topic = properties.getProperty("pubsub.topic");
            String subscription  = properties.getProperty("pubsub.subscription");

            TopicName topicName = TopicName.create(projectId, topic);

            try {
                topicAdminClient.createTopic(topicName);
            } catch (ApiException e) {
                if (io.grpc.Status.Code.ALREADY_EXISTS != e.getStatusCode()) {
                    throw e;
                } else {
                    LOG.info("Topic " + topic + " already exist");
                }
            }
            SubscriptionName subscriptionName =
                    SubscriptionName.create(projectId, subscription);
            // create a pull subscription with default acknowledgement deadline

            try {
                subscriptionAdminClient.createSubscription(
                        subscriptionName, topicName, PushConfig.getDefaultInstance(), ackDeadlineSeconds);
            } catch (ApiException e) {
                if (io.grpc.Status.Code.ALREADY_EXISTS != e.getStatusCode()) {
                    throw e;
                } else {
                    LOG.info("Subscription " + subscription + " already exist");
                }
            }
        }
    }

}
