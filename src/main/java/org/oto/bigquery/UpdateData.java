package org.oto.bigquery;

// [START bigquery_update_with_dml]
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

// Sample to update data in BigQuery tables using DML
public class UpdateData {

    public static void main(String[] args) throws IOException, InterruptedException {
        // Replace with your actual dataset/table.
        String datasetName = "test_dataset";
        String tableName = "Clients";
        updateTableDml(datasetName, tableName);
    }

    public static void updateTableDml(String datasetName, String tableName)
            throws IOException, InterruptedException {
        try {
            // 1) Initialize the BigQuery client
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            // 2) Define the table ID
            TableId tableId = TableId.of(datasetName, tableName);

            // 3) (Optional) Load data from a local JSON file into the Clients table
            //    If you don't need to load data first, comment out or remove this block.
            WriteChannelConfiguration writeChannelConfiguration =
                    WriteChannelConfiguration.newBuilder(tableId)
                            .setFormatOptions(FormatOptions.json()) // We assume the file is JSON
                            .build();

            // Adjust the path and file name as needed (e.g., "clientsData.json")
            Path jsonPath = FileSystems.getDefault().getPath("src/test/resources", "clientsData.json");

            String jobName = "jobId_" + UUID.randomUUID();
            JobId jobId = JobId.newBuilder().setLocation("us").setJob(jobName).build();

            try (
                    TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
                    OutputStream stream = Channels.newOutputStream(writer)
            ) {
                Files.copy(jsonPath, stream);
            }

            // 4) Wait for the load job to complete
            Job loadJob = bigquery.getJob(jobId);
            Job completedJob = loadJob.waitFor();
            if (completedJob == null) {
                System.out.println("Job not executed since it no longer exists.");
                return;
            } else if (completedJob.getStatus().getError() != null) {
                System.out.println("BigQuery was unable to load the local file due to an error: \n"
                        + completedJob.getStatus().getError());
                return;
            }
            System.out.println(completedJob.getStatistics()
                    + " JSON file uploaded successfully into Clients table.");

            // 5) Run a DML statement to update rows in the Clients table
            // Example: Masking any gmail.com addresses to a generic domain
            // Adjust the SET/WHERE as needed for your logic
            String dmlQuery =
                    String.format(
                            "UPDATE `%s.%s` "
                                    + "SET clientEmail = 'redacted@mycompany.com' "
                                    + "WHERE clientEmail LIKE '%%@gmail.com'",
                            datasetName, tableName);

            QueryJobConfiguration dmlQueryConfig = QueryJobConfiguration.newBuilder(dmlQuery).build();

            // 6) Execute the DML query
            TableResult result = bigquery.query(dmlQueryConfig);

            // 7) (Optional) Print returned rows, if any
            result.iterateAll().forEach(row ->
                    row.forEach(fieldValue -> System.out.println(fieldValue.getValue()))
            );

            System.out.println("Table updated successfully using DML.");

        } catch (BigQueryException e) {
            System.out.println("Table update failed \n" + e.toString());
        }
    }
}
// [END bigquery_update_with_dml]
