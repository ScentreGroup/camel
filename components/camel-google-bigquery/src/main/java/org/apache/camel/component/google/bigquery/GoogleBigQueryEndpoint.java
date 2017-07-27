package org.apache.camel.component.google.bigquery;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.util.ResourceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BigQuery Endpoint Definition
 * Represents a table within a BigQuery dataset
 * Contains configuration details for a single table and the utility methods (such as check, create) to ease operations
 * URI Parameters:
 * * Logger ID - To ensure that logging is unified under Route Logger, the logger ID can be passed on
 *               via an endpoint URI parameter
 * * Partitioned - to indicate that the table needs to be partitioned - every UTC day to be written into a
 *                 timestamped separate table
 *                 side effect: Australian operational day is always split between two UTC days, and, therefore, tables
 *
 * Another consideration is that exceptions are not handled within the class. They are expected to bubble up and be handled
 * by Camel.
 */
@UriEndpoint(scheme = "bigquery",title = "BigQuery", syntax = "bigquery:projectId:dataSetId[:tableName]")
public class GoogleBigQueryEndpoint extends DefaultEndpoint {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @UriParam
    protected final GoogleBigQueryConfiguration configuration;

    private Bigquery bigquery;
    private String uri;

    private Map<String, Boolean> verifiedTables = new ConcurrentHashMap<>();
    private ExecutorService executorService;

    protected GoogleBigQueryEndpoint(String uri, Bigquery bigquery, GoogleBigQueryConfiguration configuration) {
        this.bigquery = bigquery;
        this.uri = uri;
        this.configuration = configuration;
    }

    public Producer createProducer() throws Exception {
        GoogleBigQueryProducer producer = new GoogleBigQueryProducer(this, configuration);
        if (configuration.getConcurrentConsumers() > 0) {
            executorService = getCamelContext()
                    .getExecutorServiceManager()
                    .newFixedThreadPool(
                            this,
                            "camel-google-bigquery",
                            configuration.getConcurrentConsumers()
                    );
        } else {
            executorService = getCamelContext()
                    .getExecutorServiceManager()
                    .newDefaultThreadPool(this, "camel-google-bigquery");
        }
        return producer;
    }


    public Consumer createConsumer(Processor processor) throws Exception {
        throw new UnsupportedOperationException("Cannot consume from the BigQuery endpoint: " + getEndpointUri());
    }

    void checkOrCreateBQTable(String tableId) throws Exception {
        if (configuration.isCreateTable()) {
            verifiedTables.computeIfAbsent(tableId, (tableName) -> {
                try {
                    if (!checkBqTableExists(tableId)) {
                        createBqTable(tableId);
                    }
                    return true;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private String createBqTable(String actualTableName) throws Exception {
        TableReference reference = new TableReference()
            .setTableId(actualTableName)
            .setDatasetId(configuration.getDatasetId())
            .setProjectId(configuration.getProjectId());
        InputStream schemaInputStream = ResourceHelper.resolveMandatoryResourceAsInputStream(getCamelContext(), configuration.getSchemaLocation());
        TableSchema schema = GoogleBigQuerySchemaReader.readDefinition(schemaInputStream);
        Table table = new Table()
            .setTableReference(reference)
            .setSchema(schema);
        if (configuration.isPartitioned()) {
            TimePartitioning timePartitioning = new TimePartitioning();
            // Onyl type supported currently
            timePartitioning.setType("DAY");
            table = table.setTimePartitioning(timePartitioning);
        }
        bigquery.tables()
            .insert(configuration.getProjectId(), configuration.getDatasetId(), table)
            .execute();
        return actualTableName;
    }

    // Google API is paginated.
    // Implementing two approaches at the same time to address that.
    // 1. Setting Max Results per page to 5000
    // 2. Iterating through pages, in case there is still an implied limit on Max Results
    //    suspicion is that it might be set to 500.
    private boolean checkBqTableExists(String tableName) throws Exception {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setQuery("SELECT COUNT(1) AS cnt\n" +
                "FROM " + configuration.getDatasetId() + ".__TABLES_SUMMARY__\n" +
                "WHERE table_id = '" + tableName + "'");

        Bigquery.Jobs.Query query = bigquery.jobs().query(configuration.getProjectId(), queryRequest);
        QueryResponse response = query.execute();
        return "1".equals(response.getRows().get(0).getF().get(0).getV());
    }

    public boolean isSingleton() {
        return false;
    }

    public Bigquery getBigquery() {
        return bigquery;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    @Override
    protected String createEndpointUri() {
        return uri;
    }

    @Override
    public GoogleBigQueryComponent getComponent() {
        return (GoogleBigQueryComponent)super.getComponent();
    }


}
