package org.apache.camel.component.google.bigquery.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import org.apache.camel.Endpoint;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.google.bigquery.BigQueryTestSupport;
import org.apache.camel.component.google.bigquery.GoogleBigQueryConstants;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

public class SingleRowTest extends BigQueryTestSupport {
    private static final String TABLE_ID = "singlerow";

    @EndpointInject(uri = "direct:in")
    private Endpoint directIn;

    @EndpointInject(uri = "google-bigquery:{{project.id}}:{{bigquery.datasetId}}:" + TABLE_ID + "?createTable=true&schemaLocation=classpath:/schema/singlerow.json")
    private Endpoint bigqueryEndpoint;

    @EndpointInject(uri = "mock:sendResult")
    private MockEndpoint sendResult;

    @Produce(uri = "direct:in")
    private ProducerTemplate producer;

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() {
                from(directIn)
                        .routeId("SingleRow")
                        .to(bigqueryEndpoint)
                .to(sendResult);
            }
        };
    }

    @Test
    public void singleMessage() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        String uuidCol1 = UUID.randomUUID().toString();
        String uuidCol2 = UUID.randomUUID().toString();

        Map<String, String> object = new HashMap<>();
        object.put("col1", uuidCol1);
        object.put("col2", uuidCol2);
        exchange.getIn().setBody(object);

        sendResult.expectedMessageCount(1);
        producer.send(exchange);
        sendResult.assertIsSatisfied(4000);

        assertRowExist(TABLE_ID, object);
    }

}
