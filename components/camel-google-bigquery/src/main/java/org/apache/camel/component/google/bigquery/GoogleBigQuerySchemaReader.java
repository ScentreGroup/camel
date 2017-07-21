package org.apache.camel.component.google.bigquery;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.*;

/**
 * The class is used to read bigquery schema definitions
 * from the classpath.
 * Used when the table is not found and has to be created automatically by the solution
 */

public class GoogleBigQuerySchemaReader {

    public static final String DDL_SUFFIX=".json";

    public static TableSchema readDefinition(String location, String tableName) throws Exception {
        TableSchema schema = new TableSchema();

        InputStream in = GoogleBigQuerySchemaReader.class
            .getClassLoader()
            .getResourceAsStream(location + "/" + tableName+DDL_SUFFIX);

        ObjectMapper mapper = new ObjectMapper();
        List<TableFieldSchema> fields = mapper.readValue(in, ArrayList.class);

        schema.setFields(fields);

        return schema;
    }

}
