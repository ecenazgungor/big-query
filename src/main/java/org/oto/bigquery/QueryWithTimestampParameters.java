package org.oto.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableResult;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.ZoneOffset;
import org.threeten.bp.ZonedDateTime;

// Sample to running a query with timestamp query parameters.
public class QueryWithTimestampParameters {

    public static void main(String[] args) {
        queryWithTimestampParameters();
    }

    public static void queryWithTimestampParameters() {
        try {
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            ZonedDateTime timestamp = LocalDateTime.of(2016, 12, 7, 8, 0, 0).atZone(ZoneOffset.UTC);
            String query = "SELECT TIMESTAMP_ADD(@ts_value, INTERVAL 1 HOUR);";
            // Note: Standard SQL is required to use query parameters.
            QueryJobConfiguration queryConfig =
                    QueryJobConfiguration.newBuilder(query)
                            .addNamedParameter(
                                    "ts_value",
                                    QueryParameterValue.timestamp(
                                            // Timestamp takes microseconds since 1970-01-01T00:00:00 UTC
                                            timestamp.toInstant().toEpochMilli() * 1000))
                            .build();

            TableResult results = bigquery.query(queryConfig);

            results
                    .iterateAll()
                    .forEach(row -> row.forEach(val -> System.out.printf("%s", val.toString())));

            System.out.println("Query with timestamp parameter performed successfully.");
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Query not performed \n" + e.toString());
        }
    }
}