package org.apache.camel.component.google.bigquery;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic BigQuery Producer
 */
public class GoogleBigQueryProducer extends DefaultProducer implements Processor {

    private Logger logger;

    public GoogleBigQueryProducer(GoogleBigQueryEndpoint endpoint) throws Exception {
        super(endpoint);

        String loggerId = endpoint.getLoggerId();
        if (loggerId == null || loggerId.trim().isEmpty())
            loggerId = this.getClass().getName();

        logger = LoggerFactory.getLogger(loggerId);

    }

    /**
     * The method converts a single incoming message into a List
     *
     * @param exchange
     * @return
     */
    private static List<Exchange> prepareExchangeList(Exchange exchange) {

        List<Exchange> entryList = null;

        if (null == exchange.getProperty(Exchange.GROUPED_EXCHANGE)) {
            entryList = new ArrayList<>();
            entryList.add(exchange);
        } else {
            entryList = (List<Exchange>) exchange.getProperty(Exchange.GROUPED_EXCHANGE);
        }

        return entryList;
    }

    /**
     * The incoming message is expected to be a List of Maps
     * The assumptions:
     * - All incoming records go into the same table
     * - Incoming records sorted by the timestamp
     * If required this can be modified to a version where the split can be performed mid array.
     * <p/>
     * Data : Expects Array<Map> where each Map record would contain a "timestamp" field
     */
    @Override
    public void process(Exchange exchange) throws Exception {
        List<Exchange> exchanges = prepareExchangeList(exchange);

        Long timestamp = null;



        List<TableDataInsertAllRequest.Rows> apiRequestRows = new ArrayList<>();





        for (Exchange ex: exchanges) {
            Object entryObject = ex.getIn();
            if (entryObject instanceof List) {
                for(Map<String, Object> entry: (List<Map<String, Object>>) entryObject) {
                    if(timestamp == null) {
                        timestamp = getTimestamp(entry);
                    }
                    TableRow tableRow = new TableRow();
                    tableRow.putAll(entry);
                    logger.trace("Processing Entry : {}", entryObject);
                    apiRequestRows.add(new TableDataInsertAllRequest.Rows().setJson(tableRow));
                }
            } else if (entryObject instanceof Map) {
                if(timestamp == null) {
                    timestamp = getTimestamp((Map<String, Object>)entryObject);
                }
                TableRow tableRow = new TableRow();
                tableRow.putAll((Map)entryObject);
                logger.trace("Processing Entry : {}", entryObject);
                apiRequestRows.add(new TableDataInsertAllRequest.Rows().setJson(tableRow));
            }
        }
        if (apiRequestRows.isEmpty()) {
            logger.debug("The incoming message is either null or empty for exchange {}", exchange.getExchangeId());
            return;
        }

        TableDataInsertAllRequest apiRequestData = new TableDataInsertAllRequest().setRows(apiRequestRows);

        GoogleBigQueryEndpoint endpoint = (GoogleBigQueryEndpoint)getEndpoint();
        Bigquery.Tabledata.InsertAll apiRequest = endpoint.getBigquery()
            .tabledata()
            .insertAll(endpoint.getProjectId(),
                endpoint.getDatasetId(),
                endpoint.getTableNameForTimestamp(timestamp),     //Getting the actualTableName
                apiRequestData);

        logger.trace("uploader thread/id: {} / {} . calling google api", Thread.currentThread(),exchange.getExchangeId());
        if (logger.isTraceEnabled())
            logger.trace("uploader thread/id: " + Thread.currentThread()
                .getId() + " / " + exchange.getExchangeId() + " . calling google api");

        TableDataInsertAllResponse apiResponse = apiRequest.execute();

        if (apiResponse.getInsertErrors() != null && !apiResponse.getInsertErrors().isEmpty()) {
            throw new Exception("InsertAll failed: " + apiResponse.getInsertErrors());
        }

        logger.debug("uploader thread/id: {} / {} . api call completed.", Thread.currentThread().getId(), exchange.getExchangeId());
    }

    private long getTimestamp(Map<String, Object> map) {
        Object firstTimestamp = map.get("timestamp");
        long timestamp;
        if (firstTimestamp instanceof Long)
            timestamp = (Long) firstTimestamp;
        else if (firstTimestamp instanceof String)
            timestamp = new Long((String) firstTimestamp);
        else
            timestamp = new Date().getTime();
        return timestamp;
    }


    @Override
    public boolean isSingleton() {
        return true;
    }
}
