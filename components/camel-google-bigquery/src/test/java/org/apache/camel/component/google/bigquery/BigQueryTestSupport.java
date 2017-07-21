package org.apache.camel.component.google.bigquery;

import java.io.InputStream;
import java.util.Properties;

import org.apache.camel.CamelContext;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.test.junit4.CamelTestSupport;

public class BigQueryTestSupport extends CamelTestSupport {
    public static final String SERVICE_KEY;
    public static final String SERVICE_ACCOUNT;
    public static final String PROJECT_ID;
    public static final String SERVICE_URL;

    static {
        Properties testProperties = loadProperties();
        SERVICE_KEY = testProperties.getProperty("service.key");
        SERVICE_ACCOUNT = testProperties.getProperty("service.account");
        PROJECT_ID = testProperties.getProperty("project.id");
        SERVICE_URL = testProperties.getProperty("test.serviceURL");
    }

    private static Properties loadProperties() {
        Properties testProperties = new Properties();
        InputStream fileIn = BigQueryTestSupport.class.getClassLoader().getResourceAsStream("simple.properties");
        try {
            testProperties.load(fileIn);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return testProperties;
    }

    protected void addBigqueryComponent(CamelContext context) {

        GoogleBigQueryConnectionFactory cf = new GoogleBigQueryConnectionFactory()
                .setServiceAccount(SERVICE_ACCOUNT)
                .setServiceAccountKey(SERVICE_KEY)
                .setServiceURL(SERVICE_URL);

        GoogleBigQueryComponent component = new GoogleBigQueryComponent();
        component.setConnectionFactory(cf);

        context.addComponent("google-bigquery", component);
        context.addComponent("properties", new PropertiesComponent("ref:prop"));
    }

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();
        addBigqueryComponent(context);
        return context;
    }

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry jndi = super.createRegistry();
        jndi.bind("prop", loadProperties());
        return jndi;
    }
}
