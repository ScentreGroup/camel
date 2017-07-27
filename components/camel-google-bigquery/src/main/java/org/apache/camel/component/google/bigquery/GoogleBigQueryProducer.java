package org.apache.camel.component.google.bigquery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultAsyncProducer;
import org.apache.camel.impl.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic BigQuery Producer
 */
public class GoogleBigQueryProducer extends DefaultAsyncProducer {

    private Logger logger;
    private GoogleBigQueryConfiguration configuration;

    public GoogleBigQueryProducer(GoogleBigQueryEndpoint endpoint, GoogleBigQueryConfiguration configuration) throws Exception {
        super(endpoint);

        this.configuration = configuration;

        String loggerId = configuration.getLoggerId();
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
     * Process the exchange
     *
     * The incoming exchange can be a grouped exchange in which case all the exchanges will be combined.
     *
     * The incoming can be
     * <ul>
     *     <li>A map where all map keys will map to field records. One map object maps to one bigquery row</li>
     *     <li>A list of objects. Each entry in the list will map to one bigquery row</li>
     * </ul>
     * The incoming message is expected to be a List of Maps
     * The assumptions:
     * - All incoming records go into the same table
     * - Incoming records sorted by the timestamp
     */
    @Override
    public void process(Exchange exchange) throws Exception {
        List<Exchange> exchanges = prepareExchangeList(exchange);

        List<TableDataInsertAllRequest.Rows> apiRequestRows = new ArrayList<>();
        List<Exchange> processGroup = new ArrayList<>();
        String tablename = null;
        String suffix = null;
        int totalProcessed = 0;

        for (Exchange ex: exchanges) {
            String tableNameFromHeader = exchange.getIn().getHeader(GoogleBigQueryConstants.TABLE_NAME, String.class);
            String tmpTablename = tableNameFromHeader != null ? tableNameFromHeader : configuration.getTableId();
            String tmpSuffix = exchange.getIn().getHeader(GoogleBigQueryConstants.TABLE_SUFFIX, String.class);

            // Ensure all rows of same request goes to same table and suffix
            if (!tmpTablename.equals(tablename) || !tmpSuffix.equals(suffix)) {
                if (!processGroup.isEmpty()) {
                    totalProcessed += process(tablename, suffix, processGroup, exchange.getExchangeId());
                }
                processGroup.clear();
                tablename = tmpTablename;
                suffix = tmpSuffix;
            }
            processGroup.add(ex);
        }
        if (!processGroup.isEmpty()) {
            totalProcessed += process(tablename, suffix, processGroup, exchange.getExchangeId());
        }

        if (totalProcessed == 0) {
            logger.debug("The incoming message is either null or empty for exchange {}", exchange.getExchangeId());
        }
    }

    private int process(String tableId, String suffix, List<Exchange> exchanges, String exchangeId) throws Exception {
        List<TableDataInsertAllRequest.Rows> apiRequestRows = new ArrayList<>();
        for (Exchange ex: exchanges) {
            Object entryObject = ex.getIn().getBody();
            if (entryObject instanceof List) {
                for(Map<String, Object> entry: (List<Map<String, Object>>) entryObject) {
                    apiRequestRows.add(createRowRequest(null, entry));
                }
            } else if (entryObject instanceof Map) {
                apiRequestRows.add(createRowRequest(ex, (Map<String, Object>) entryObject));
            } else {
                ex.setException(new IllegalArgumentException("Cannot handle body type " + entryObject.getClass()));
            }
        }

        if (apiRequestRows.isEmpty()) {
            return 0;
        }

        GoogleBigQueryEndpoint endpoint = getEndpoint();

        endpoint.checkOrCreateBQTable(tableId);
        TableDataInsertAllRequest apiRequestData = new TableDataInsertAllRequest().setRows(apiRequestRows);

        String insertId = null;
        if (configuration.getUseAsInsertId() != null) {
            Object id = (String) apiRequestRows.get(0).get(configuration.getUseAsInsertId());
            if (id instanceof String) {
                insertId = (String) id;
            }
        } else {
            insertId = exchanges.get(0).getIn().getHeader(GoogleBigQueryConstants.INSERT_ID_HEADER_NAME, String.class);
        }
        if (insertId != null) {
            apiRequestData.set("insertId", insertId);
        }

        Bigquery.Tabledata.InsertAll apiRequest = endpoint.getBigquery()
                .tabledata()
                .insertAll(configuration.getProjectId(),
                        configuration.getDatasetId(),
                        tableId,     //Getting the actualTableName
                        apiRequestData);
        if (suffix != null) {
            apiRequest = apiRequest.set("template_suffix", suffix);
        }

        logger.trace("uploader thread/id: {} / {} . calling google api", Thread.currentThread(), exchangeId);

        TableDataInsertAllResponse apiResponse = apiRequest.execute();

        if (apiResponse.getInsertErrors() != null && !apiResponse.getInsertErrors().isEmpty()) {
            throw new Exception("InsertAll into " + tableId + " failed: " + apiResponse.getInsertErrors());
        }

        logger.debug("uploader thread/id: {} / {} . api call completed.", Thread.currentThread().getId(), exchangeId);
        return apiRequestData.size();
    }

    private TableDataInsertAllRequest.Rows createRowRequest(Exchange exchange, Map<String, Object> object) {
        TableRow tableRow = new TableRow();
        tableRow.putAll(object);
        String insertId = null;
        if (configuration.getUseAsInsertId() != null) {
            insertId = (String)(object.get(configuration.getUseAsInsertId()));
        } else {
            if (exchange != null) {
                insertId = exchange.getIn().getHeader(GoogleBigQueryConstants.INSERT_ID_HEADER_NAME, String.class);
            }
        }
        TableDataInsertAllRequest.Rows rows = new TableDataInsertAllRequest.Rows();
        rows.setInsertId(insertId);
        rows.setJson(tableRow);
        return rows;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public GoogleBigQueryEndpoint getEndpoint() {
        return (GoogleBigQueryEndpoint) super.getEndpoint();
    }

    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        getEndpoint().getExecutorService().submit(() -> {
            try {
                process(exchange);
            } catch (Exception e) {
                exchange.setException(e);
            }
            callback.done(false);
        });
        return false;
    }
}
