package org.oto.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertingDataTypes {

    public static void main(String[] args) {

        String datasetName = "test_dataset";
        String tableName = "Clients";
        insertingDataTypes(datasetName, tableName);
    }

    public static void insertingDataTypes(String datasetName, String tableName) {
        try {
            // Initialize BigQuery service
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            // 1) Define fields for the Clients table
            Field idField = Field.newBuilder("id", StandardSQLTypeName.INT64)
                    .setMode(Field.Mode.REPEATED)
                    .build();
            Field companyIdField = Field.newBuilder("companyId", StandardSQLTypeName.INT64)
                    .setMode(Field.Mode.REPEATED)
                    .build();
            Field activeField = Field.of("active", StandardSQLTypeName.BOOL);
            Field clientNameField = Field.of("clientName", StandardSQLTypeName.STRING);
            Field clientPhoneField = Field.of("clientPhone", StandardSQLTypeName.STRING);
            Field clientEmailField = Field.of("clientEmail", StandardSQLTypeName.STRING);
            Field clientTypeField = Field.of("clientType", StandardSQLTypeName.STRING);
            Field createdByField = Field.of("createdBy", StandardSQLTypeName.STRING);
            Field createdAtField = Field.of("createdAt", StandardSQLTypeName.DATETIME);

            // 2) Build schema
            Schema schema = Schema.of(
                    idField,
                    companyIdField,
                    activeField,
                    clientNameField,
                    clientPhoneField,
                    clientEmailField,
                    clientTypeField,
                    createdByField,
                    createdAtField
            );

            // 3) Create (or overwrite) the table with this schema
            TableId tableId = TableId.of(datasetName, tableName);
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

            // This will create a new table if it doesn't exist.
            // If the table already exists, you may get an error or overwrite it
            // depending on your BigQuery settings.
            bigquery.create(tableInfo);

            // 4) Prepare a row of data to insert
            Map<String, Object> rowContent = new HashMap<>();
            // For REPEATED (array) columns, pass an array or List
            rowContent.put("id", new Long[] {1001L});
            rowContent.put("companyId", new Long[] {5001L, 5002L}); // Example: multiple companyIds
            rowContent.put("active", true);
            rowContent.put("clientName", "Ecenaz Gungor");
            rowContent.put("clientPhone", "+905351111111");
            rowContent.put("clientEmail", "ecenaz.gungor@gmail.com");
            rowContent.put("clientType", "Scale");
            rowContent.put("createdBy", "adminUser");
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String nowString = LocalDateTime.now().format(dtf);
            rowContent.put("createdAt", nowString);

            // 5) Insert the row via streaming API
            InsertAllRequest request = InsertAllRequest.newBuilder(tableId)
                    .addRow(rowContent)
                    .build();

            InsertAllResponse response = bigquery.insertAll(request);

            // 6) Check for insertion errors
            if (response.hasErrors()) {
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    System.out.println("Error inserting row " + entry.getKey() + ": " + entry.getValue());
                }
            } else {
                System.out.println("Row successfully inserted into Clients table.");
            }

        } catch (BigQueryException e) {
            System.out.println("Insert operation not performed \n" + e.toString());
        }
    }
}
