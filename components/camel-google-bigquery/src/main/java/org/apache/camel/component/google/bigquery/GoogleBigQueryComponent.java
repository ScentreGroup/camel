package org.apache.camel.component.google.bigquery;

import java.util.Map;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;

public class GoogleBigQueryComponent extends DefaultComponent {

    private Bigquery bigQuery = null;
    private String projectId;
    private String datasetId;

    private GoogleBigQueryConnectionFactory connectionFactory;

    private final HttpTransport transport = new NetHttpTransport();
    private final JsonFactory jsonFactory = new JacksonFactory();

    public Bigquery getConnection() throws Exception {
        return getConnectionFactory().getDefaultClient();
    }


    // Endpoint represents a single table
    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        String[] parts = remaining.split(":");

        if (parts.length < 3) {
            throw new IllegalArgumentException("Google BigQuery Endpoint format \"projectId:datasetId:tableName\"");
        }

        GoogleBigQueryConfiguration configuration = new GoogleBigQueryConfiguration();
        setProperties(configuration, parameters);
        configuration.parseRemaining(remaining);

        if (configuration.getConnectionFactory() == null) {
            configuration.setConnectionFactory(getConnectionFactory());
        }

        if (bigQuery == null) {
            bigQuery = getConnection();
        }

        GoogleBigQueryEndpoint bigQueryEndpoint = new GoogleBigQueryEndpoint(uri, bigQuery, configuration);

        return bigQueryEndpoint;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getDatasetId() {
        return datasetId;
    }

    /**
     * Sets the connection factory to use:
     * provides the ability to explicitly manage connection credentials:
     * - the path to the key file
     * - the Service Account Key / Email pair
     */
    public GoogleBigQueryConnectionFactory getConnectionFactory() {
        if (connectionFactory == null) {
            connectionFactory = new GoogleBigQueryConnectionFactory();
        }
        return connectionFactory;
    }

    public void setConnectionFactory(GoogleBigQueryConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }
}
