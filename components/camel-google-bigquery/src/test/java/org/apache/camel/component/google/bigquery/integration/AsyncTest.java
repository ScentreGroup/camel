package org.apache.camel.component.google.bigquery.integration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.camel.Endpoint;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.google.bigquery.BigQueryTestSupport;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultExchange;
import org.junit.Test;

public class AsyncTest extends BigQueryTestSupport {
    private static final String TABLE_ID = "asynctest";

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
                        .to("seda:seda");
                from("seda:seda")
                        .routeId("Async")
                        //.threads(10)

                        .inOnly(bigqueryEndpoint)
                        .log(LoggingLevel.INFO, "To sendresult")
                .to(sendResult);
            }
        };
    }

    @Test
    public void singleMessage() throws Exception {
        List<Map> objects = new ArrayList<>();
        for(int i = 0; i < 10; i++) {
            Exchange exchange = new DefaultExchange(context);
            String uuidCol1 = UUID.randomUUID().toString();
            String uuidCol2 = UUID.randomUUID().toString();

            Map<String, String> object = new HashMap<>();
            object.put("col1", uuidCol1);
            object.put("col2", uuidCol2);
            objects.add(object);
            exchange.getIn().setBody(object);
            producer.send(exchange);
        }
        sendResult.expectedMessageCount(10);

        sendResult.assertIsSatisfied(4000);

        for (Map object: objects) {
            assertRowExist(TABLE_ID, object);
        }
    }

}
