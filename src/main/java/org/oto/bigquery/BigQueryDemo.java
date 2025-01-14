package org.oto.bigquery;


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

public class BigQueryDemo {

    private static final String PROJECT_ID = "oto-rest-api";
    private static final String DATASET_NAME = "test_dataset";
    private static final String TABLE_NAME = "Clients";

    public static void main(String[] args) throws Exception {
        // Instantiate a client
        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

        // 1. INSERT new rows
        insertRows(bigQuery);

        // 2. UPDATE existing rows
        updateRows(bigQuery);

        // 3. DELETE specific rows
        deleteRows(bigQuery);
    }

    private static void insertRows(BigQuery bigQuery) throws Exception {
        // Example: Insert via DML
        // Adjust columns and values as needed.
        String dml = String.format(
                "INSERT `%s.%s.%s` (id, name, email) VALUES (1001, 'John Doe', 'john.doe@example.com')",
                PROJECT_ID, DATASET_NAME, TABLE_NAME
        );

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(dml).build();
        TableResult result = bigQuery.query(queryConfig);

        System.out.println("Insert completed. Rows inserted: " + result.getTotalRows());
    }

    private static void updateRows(BigQuery bigQuery) throws Exception {
        // Example: Update email for a row with id = 1001
        String dml = String.format(
                "UPDATE `%s.%s.%s` " +
                        "SET email = 'john.doe@newdomain.com' " +
                        "WHERE id = 1001",
                PROJECT_ID, DATASET_NAME, TABLE_NAME
        );

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(dml).build();
        TableResult result = bigQuery.query(queryConfig);

        System.out.println("Update completed. Rows updated: " + result.getTotalRows());
    }

    private static void deleteRows(BigQuery bigQuery) throws Exception {
        // Example: Delete the row with id = 1001
        String dml = String.format(
                "DELETE FROM `%s.%s.%s` WHERE id = 1001",
                PROJECT_ID, DATASET_NAME, TABLE_NAME
        );

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(dml).build();
        TableResult result = bigQuery.query(queryConfig);

        System.out.println("Delete completed. Rows deleted: " + result.getTotalRows());
    }
}
