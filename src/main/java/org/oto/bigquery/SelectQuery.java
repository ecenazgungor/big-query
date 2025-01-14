package org.oto.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

// [START bigquery_query]
public class SelectQuery {

    public static void main(String[] args) {
        // Example query: Group clients by clientType and get counts.
        // Replace YOUR_PROJECT_ID with your actual project ID (e.g., "oto-rest-api").
        // If your default project is already set correctly, you can omit the project ID from the table reference.
        String query =
                "SELECT clientType, COUNT(*) AS totalClients "
                        + "FROM `YOUR_PROJECT_ID.test_dataset.Clients` "
                        + "GROUP BY clientType";

        simpleQuery(query);
    }

    public static void simpleQuery(String query) {
        try {
            // Initialize the BigQuery client (Application Default Credentials).
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            // Build the query config
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            // Execute the query
            TableResult result = bigquery.query(queryConfig);

            // Print the results
            result
                    .iterateAll()
                    .forEach(row -> {
                        // Safely handle potential NULL fields (e.g., if clientType is missing)
                        String clientType = row.get("clientType") == null
                                ? "UNKNOWN"
                                : row.get("clientType").getStringValue();
                        long count = row.get("totalClients").getLongValue();
                        System.out.println("clientType: " + clientType + ", totalClients: " + count);
                    });

            System.out.println("Query ran successfully.");
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Query did not run. \n" + e);
        }
    }
}
// [END bigquery_query]
