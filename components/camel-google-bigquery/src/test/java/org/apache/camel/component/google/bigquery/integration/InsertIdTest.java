package org.apache.camel.component.google.bigquery.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
import org.junit.Test;

public class InsertIdTest extends BigQueryTestSupport {
    private static final String TABLE_ID = "insertId";

    @EndpointInject(uri = "direct:withInsertId")
    private Endpoint directInWithInsertId;

    @EndpointInject(uri = "direct:in")
    private Endpoint directIn;

    @EndpointInject(uri = "google-bigquery:{{project.id}}:{{bigquery.datasetId}}:"
            + TABLE_ID
            + "?createTable=true&schemaLocation=classpath:/schema/singlerow.json&useAsInsertId=col1")
    private Endpoint bigqueryEndpointWithInsertId;

    @EndpointInject(uri = "google-bigquery:{{project.id}}:{{bigquery.datasetId}}:"
            + TABLE_ID
            + "?createTable=true&schemaLocation=classpath:/schema/singlerow.json")
    private Endpoint bigqueryEndpoint;

    @EndpointInject(uri = "mock:sendResult")
    private MockEndpoint sendResult;

    @EndpointInject(uri = "mock:sendResultWithInsertId")
    private MockEndpoint sendResultWithInsertId;

    @Produce(uri = "direct:withInsertId")
    private ProducerTemplate producerWithInsertId;

    @Produce(uri = "direct:in")
    private ProducerTemplate producer;

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() {
                from(directInWithInsertId)
                        .routeId("SingleRowWithInsertId")
                        .to(bigqueryEndpointWithInsertId)
                .to(sendResultWithInsertId);

                from(directIn)
                        .routeId("SingleRow")
                        .to(bigqueryEndpointWithInsertId)
                        .to(sendResult);
            }
        };
    }

    @Test
    public void sendTwoMessagesExpectOneRow() throws Exception {

        Exchange exchange = new DefaultExchange(context);
        String uuidCol1 = UUID.randomUUID().toString();
        String uuidCol2 = UUID.randomUUID().toString();

        Map<String, String> object = new HashMap<>();
        object.put("col1", uuidCol1);
        object.put("col2", uuidCol2);
        exchange.getIn().setBody(object);

        Exchange exchange2 = new DefaultExchange(context);
        String uuid2Col2 = UUID.randomUUID().toString();

        Map<String, String> object2 = new HashMap<>();
        object.put("col1", uuidCol1);
        object.put("col2", uuid2Col2);
        exchange2.getIn().setBody(object);


        sendResultWithInsertId.expectedMessageCount(2);
        producerWithInsertId.send(exchange);
        producerWithInsertId.send(exchange2);
        sendResultWithInsertId.assertIsSatisfied(4000);

        assertRowExist(TABLE_ID, object);
    }


    @Test
    public void sendTwoMessagesExpectOneRowUsingExchange() throws Exception {

        Exchange exchange = new DefaultExchange(context);
        String uuidCol1 = UUID.randomUUID().toString();
        String uuidCol2 = UUID.randomUUID().toString();

        Map<String, String> object = new HashMap<>();
        object.put("col1", uuidCol1);
        object.put("col2", uuidCol2);
        exchange.getIn().setBody(object);
        exchange.getIn().setHeader(GoogleBigQueryConstants.INSERT_ID_HEADER_NAME, uuidCol1);

        Exchange exchange2 = new DefaultExchange(context);
        String uuid2Col2 = UUID.randomUUID().toString();

        Map<String, String> object2 = new HashMap<>();
        object.put("col1", uuidCol1);
        object.put("col2", uuid2Col2);
        exchange2.getIn().setBody(object);
        exchange.getIn().setHeader(GoogleBigQueryConstants.INSERT_ID_HEADER_NAME, uuidCol1);

        sendResult.expectedMessageCount(2);
        producer.send(exchange);
        producer.send(exchange2);
        sendResult.assertIsSatisfied(4000);

        assertRowExist(TABLE_ID, object);
    }


}
