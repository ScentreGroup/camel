package org.apache.camel.component.google.bigquery;

import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;

@UriParams
public class GoogleBigQueryConfiguration {
    @UriParam(name = "loggerId")
    private String loggerId = null;

    @UriParam(name = "schemaLocation")
    private String schemaLocation = "schema";

    @UriParam(name = "connectionFactory", description = "ConnectionFactory to obtain connection to Bigquery Service. If non provided the default one will be used")
    private GoogleBigQueryConnectionFactory connectionFactory;

    @UriParam(name = "createTable", description = "Create tables if it doesn't exist")
    private boolean createTable = false;

    @UriParam(name = "partitioned", description = "Create partitioned tables")
    private boolean partitioned = false;

    @UriParam(name = "userAsInsertId", description = "Field name to use as insert id")
    private String useAsInsertId = null;

    @UriParam(name = "concurrentConsumers", description = "Maximum number of simultaneous consumers when using async processing")
    private int concurrentConsumers;

    private String projectId;
    private String datasetId;
    private String tableId;

    public void parseRemaining(String remaining) {
        String[] parts = remaining.split(":");

        if (parts.length < 2) {
            throw new IllegalArgumentException("Google BigQuery Endpoint format \"projectId:datasetId[:tableName]\"");
        }

        int c = 0;
        projectId = parts[c++];
        datasetId = parts[c++];
        if (parts.length > 2) {
            tableId = parts[c++];
        }
    }

    public String getLoggerId() {
        return loggerId;
    }

    public GoogleBigQueryConfiguration setLoggerId(String loggerId) {
        this.loggerId = loggerId;
        return this;
    }
    /**
     * ConnectionFactory to obtain connection to PubSub Service. If non provided the default will be used.
     */
    public GoogleBigQueryConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(GoogleBigQueryConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public String getSchemaLocation() {
        return schemaLocation;
    }

    public void setSchemaLocation(String schemaLocation) {
        this.schemaLocation = schemaLocation;
    }

    public boolean isCreateTable() {
        return createTable;
    }

    public void setCreateTable(boolean createTable) {
        this.createTable = createTable;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    public void setPartitioned(boolean partitioned) {
        this.partitioned = partitioned;
    }

    public String getUseAsInsertId() {
        return useAsInsertId;
    }

    public void setUseAsInsertId(String useAsInsertId) {
        this.useAsInsertId = useAsInsertId;
    }

    public String getProjectId() {
        return projectId;
    }

    public GoogleBigQueryConfiguration setProjectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public GoogleBigQueryConfiguration setDatasetId(String datasetId) {
        this.datasetId = datasetId;
        return this;
    }

    public String getTableId() {
        return tableId;
    }

    public GoogleBigQueryConfiguration setTableId(String tableId) {
        this.tableId = tableId;
        return this;
    }

    public int getConcurrentConsumers() {
        return concurrentConsumers;
    }

    public GoogleBigQueryConfiguration setConcurrentConsumers(int concurrentConsumers) {
        this.concurrentConsumers = concurrentConsumers;
        return this;
    }
}
