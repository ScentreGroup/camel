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
package org.apache.camel.component.google.bigquery;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import org.apache.camel.CamelContext;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.test.junit4.CamelTestSupport;

public class BigQueryTestSupport extends CamelTestSupport {
    public static final String SERVICE_KEY;
    public static final String SERVICE_ACCOUNT;
    public static final String PROJECT_ID;
    public static final String DATASET_ID;
    public static final String SERVICE_URL;

    private GoogleBigQueryConnectionFactory connectionFactory;

    static {
        Properties testProperties = loadProperties();
        SERVICE_KEY = testProperties.getProperty("service.key");
        SERVICE_ACCOUNT = testProperties.getProperty("service.account");
        PROJECT_ID = testProperties.getProperty("project.id");
        DATASET_ID = testProperties.getProperty("bigquery.datasetId");
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

        connectionFactory = new GoogleBigQueryConnectionFactory()
                .setServiceAccount(SERVICE_ACCOUNT)
                .setServiceAccountKey(SERVICE_KEY)
                .setServiceURL(SERVICE_URL);

        GoogleBigQueryComponent component = new GoogleBigQueryComponent();
        component.setConnectionFactory(connectionFactory);

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

    public GoogleBigQueryConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    protected void assertRowExist(String tableName, Map<String, String> row) throws Exception {
        QueryRequest queryRequest = new QueryRequest();
        String query = "SELECT * FROM " + DATASET_ID + "." + tableName + " WHERE "
                + row.entrySet().stream()
                .map(e -> e.getKey() + " = '" + e.getValue() + "'")
                .collect(Collectors.joining(" AND "));
        log.debug("Query: {}", query);
        queryRequest.setQuery(query);
        QueryResponse queryResponse = getConnectionFactory()
                .getDefaultClient()

                .jobs()
                .query(PROJECT_ID, queryRequest)
                .execute();
        assertEquals(1, queryResponse.getRows().size());
    }
}
