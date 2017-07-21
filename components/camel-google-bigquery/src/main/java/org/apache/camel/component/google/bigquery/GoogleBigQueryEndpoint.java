package org.apache.camel.component.google.bigquery;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
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
@UriEndpoint(scheme = "bigquery",title = "BigQuery", syntax = "bigquery:tablename")
public class GoogleBigQueryEndpoint extends DefaultEndpoint {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Bigquery bigquery;
    private String uri;
    private String projectId;
    private String datasetId;
    private String tableId;

    private HashMap<Long, String> verifiedTables = new HashMap<Long, String>();

    @UriParam(name = "loggerId")
    private String loggerId = null;

    @UriParam(name = "partitioned")
    private Boolean partitioned = false;

    @UriParam(name = "schemaLocation")
    private String schemaLocation = "schema";

    @UriParam(name = "connectionFactory", description = "ConnectionFactory to obtain connection to Bigquery Service. If non provided the default one will be used")
    private GoogleBigQueryConnectionFactory connectionFactory;

    protected GoogleBigQueryEndpoint(String uri, Bigquery bigquery) {
        this.bigquery = bigquery;
        this.uri = uri;
    }

    public Producer createProducer() throws Exception {
        GoogleBigQueryProducer producer = new GoogleBigQueryProducer(this);

        // If not partitioned we can create the table right away
        // and the timestamp is ignored
        if (!partitioned)
            getTableNameForTimestamp( 0L );

        return producer;
    }


    public Consumer createConsumer(Processor processor) throws Exception {
        throw new UnsupportedOperationException("Cannot consume from the BigQuery endpoint: " + getEndpointUri());
    }

    public String getTableNameForTimestamp(long millis) throws Exception {
        // If the table is not partitioned, then the name has to be AS/IS
        // Otherwise, using the number of days as the bucket:
        // With dividing a whole by a whole, the fractionals are dropped
        Long bucket = partitioned ? millis / 86400L : 0L;
        String actualTableName = verifiedTables.get(bucket);

        if (actualTableName == null) {
            actualTableName = checkOrCreateBQTable(millis);
        }
        return actualTableName;
    }

    private String checkOrCreateBQTable(long millis) throws Exception {
        String actualTableName;
        Long bucket = partitioned ? millis / 86400L : 0L;
        // As we are relying on parallel processing to handle large loads
        // we need to ensure that only one thread is creating the table
        synchronized (this) {
            // Double checking if the table has not been created by another process while this thread was blocked
            // by synchronization.
            if (verifiedTables.get(bucket) != null) {
                return verifiedTables.get(bucket);
            }

            actualTableName = partitioned ? constructTableName(millis) : this.tableId;

            if (!checkBqTableExists(actualTableName)) {
                createBqTable(actualTableName);
            }

            verifiedTables.put(bucket, actualTableName);
        }
        return actualTableName;
    }

    private String constructTableName(Long millis) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = new Date(millis);
        return tableId + "_" + format.format(date);
    }

    private void createBqTable(String actualTableName) throws Exception {
        TableReference reference = new TableReference()
            .setTableId(actualTableName)
            .setDatasetId(this.datasetId)
            .setProjectId(this.projectId);
        TableSchema schema = GoogleBigQuerySchemaReader.readDefinition(this.schemaLocation, this.tableId);
        Table table = new Table()
            .setTableReference(reference)
            .setSchema(schema);
        bigquery.tables()
            .insert(projectId, datasetId, table)
            .execute();
    }

    // Google API is paginated.
    // Implementing two approaches at the same time to address that.
    // 1. Setting Max Results per page to 5000
    // 2. Iterating through pages, in case there is still an implied limit on Max Results
    //    suspicion is that it might be set to 500.
    private boolean checkBqTableExists(String tableName) throws Exception {
        Bigquery.Tables.List tablesRequest = bigquery.tables().list(projectId, datasetId);
        tablesRequest.setMaxResults(5000L);
        String nextPageToken= null;

        do {
            TableList tableList = tablesRequest.execute();
            if (tableList.getTables() == null) return false;

            for (TableList.Tables table : tableList.getTables()) {
                if (tableName.equals(table.getTableReference().getTableId()))
                    return true;
            }

            nextPageToken = tableList.getNextPageToken();
            tablesRequest.setPageToken( nextPageToken );

        }while( null != nextPageToken);
        return false;
    }

    public boolean isSingleton() {
        return false;
    }

    public Bigquery getBigquery() {
        return bigquery;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getLoggerId() {
        return loggerId;
    }

    public void setLoggerId(String loggerId) {
        this.loggerId = loggerId;
    }

    public Boolean isPartitioned() {
        return partitioned;
    }

    public void setPartitioned(Boolean partitioned) {
        this.partitioned = partitioned;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    @Override
    protected String createEndpointUri() {
        return uri;
    }

    @Override
    public GoogleBigQueryComponent getComponent() {
        return (GoogleBigQueryComponent)super.getComponent();
    }

    /**
     * ConnectionFactory to obtain connection to PubSub Service. If non provided the default will be used.
     */
    public GoogleBigQueryConnectionFactory getConnectionFactory() {
        return (null == connectionFactory)
                ? getComponent().getConnectionFactory()
                : connectionFactory;
    }

    public void setConnectionFactory(GoogleBigQueryConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }
}
