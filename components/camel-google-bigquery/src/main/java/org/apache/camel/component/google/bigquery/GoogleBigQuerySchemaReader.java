package org.apache.camel.component.google.bigquery;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.*;
import org.apache.camel.util.ResourceHelper;

/**
 * The class is used to read bigquery schema definitions
 * from the classpath.
 * Used when the table is not found and has to be created automatically by the solution
 */

public class GoogleBigQuerySchemaReader {

    public static final String DDL_SUFFIX=".json";

    public static TableSchema readDefinition(InputStream schemaInputStream) throws Exception {
        TableSchema schema = new TableSchema();

        ObjectMapper mapper = new ObjectMapper();
        List<TableFieldSchema> fields = mapper.readValue(schemaInputStream, ArrayList.class);

        schema.setFields(fields);

        return schema;
    }

}
