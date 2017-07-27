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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * The class is used to read bigquery schema definitions
 * from the classpath.
 * Used when the table is not found and has to be created automatically by the solution
 */
public interface GoogleBigQuerySchemaReader {
    static TableSchema readDefinition(InputStream schemaInputStream) throws Exception {
        TableSchema schema = new TableSchema();

        ObjectMapper mapper = new ObjectMapper();
        List<TableFieldSchema> fields = mapper.readValue(schemaInputStream, ArrayList.class);

        schema.setFields(fields);

        return schema;
    }
}
