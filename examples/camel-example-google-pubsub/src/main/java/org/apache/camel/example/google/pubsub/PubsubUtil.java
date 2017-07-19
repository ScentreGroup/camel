package org.apache.camel.example.google.pubsub;

import java.io.InputStream;
import java.util.Properties;

import org.apache.camel.component.google.pubsub.GooglePubsubComponent;
import org.apache.camel.component.google.pubsub.GooglePubsubConnectionFactory;

public class PubsubUtil {
    public static GooglePubsubComponent createComponent() {
        GooglePubsubComponent component = new GooglePubsubComponent();
        GooglePubsubConnectionFactory connectionFactory = new GooglePubsubConnectionFactory();
        Properties properties = loadProperties();
        connectionFactory.setCredentialsFileLocation(properties.getProperty("credentials.fileLocation"));
        connectionFactory.setServiceAccount(properties.getProperty("credentials.account"));
        connectionFactory.setServiceAccountKey(properties.getProperty("credentials.key"));
        connectionFactory.setPubsubEndpoint(properties.getProperty("pubsub.endpoint"));
        connectionFactory.setIsPlainTextChannel(Boolean.parseBoolean(properties.getProperty("pubsub.channel.plainText")));
        component.setConnectionFactory(connectionFactory);
        return component;
    }

    public static GooglePubsubConnectionFactory createConnectionFactory() {
        GooglePubsubConnectionFactory connectionFactory = new GooglePubsubConnectionFactory();
        Properties properties = loadProperties();
        connectionFactory.setCredentialsFileLocation(properties.getProperty("credentials.fileLocation"));
        connectionFactory.setServiceAccount(properties.getProperty("credentials.account"));
        connectionFactory.setServiceAccountKey(properties.getProperty("credentials.key"));
        return connectionFactory;
    }

    public static Properties loadProperties() {
        Properties properties = new Properties();
        InputStream fileIn = PubsubUtil.class.getClassLoader().getResourceAsStream("application.properties");
        try {
            properties.load(fileIn);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return properties;
    }


}
