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

import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;

@UriParams
public class GoogleBigQueryConfiguration {
    @UriParam(name = "concurrentConsumers", description = "Maximum number of simultaneous consumers when using async processing")
    private int concurrentConsumers;

    @UriParam(name = "connectionFactory", description = "ConnectionFactory to obtain connection to Bigquery Service. If non provided the default one will be used")
    private GoogleBigQueryConnectionFactory connectionFactory;

    @UriParam(name = "loggerId")
    private String loggerId;

    @UriParam(name = "useAsInsertId", description = "Field name to use as insert id")
    private String useAsInsertId;

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

    public int getConcurrentConsumers() {
        return concurrentConsumers;
    }

    public GoogleBigQueryConfiguration setConcurrentConsumers(int concurrentConsumers) {
        this.concurrentConsumers = concurrentConsumers;
        return this;
    }

    public String getUseAsInsertId() {
        return useAsInsertId;
    }

    public GoogleBigQueryConfiguration setUseAsInsertId(String useAsInsertId) {
        this.useAsInsertId = useAsInsertId;
        return this;
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
}
