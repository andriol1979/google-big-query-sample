package com.vut.services;

import com.google.cloud.bigquery.*;
import dnl.utils.text.table.TextTable;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class GoogleTrendQueryBuilder {
    public void buildQueryJob() throws InterruptedException {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(
                "SELECT\n" +
                        "   refresh_date AS Day,\n" +
                        "   term AS Top_Term,\n" +
                        "   rank,\n" +
                        "FROM `bigquery-public-data.google_trends.top_terms`\n" +
                        "WHERE\n" +
                        "   rank = 1\n" +
                        "   AND refresh_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK)\n" +
                        "GROUP BY Day, Top_Term, rank\n" +
                        "ORDER BY Day DESC")
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = BigQueryBuilder.getInstance().create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.
        TableResult result = queryJob.getQueryResults();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy")
                .withZone(ZoneId.systemDefault());
        // Print all pages of the results.
        //print table header
        System.out.format("%-14s%-40s%s\n", "Day", "Top Term", "Rank");
        System.out.println("---------------------------------------------------------");
        for (FieldValueList row : result.iterateAll()) {
            // String type
//            Instant day = row.get("Day").getTimestampInstant();
            String day = row.get("Day").getStringValue();
            String topTerm = row.get("Top_Term").getStringValue();
            Long rank = row.get("rank").getLongValue();
            System.out.format("%-14s%-40s%s\n", day, topTerm, rank);
        }
        System.out.println();
        System.out.println("---------------Print results with j-text-utils-------------------");
        List<Object[]> listResults = new ArrayList<>();
        for (FieldValueList row : result.iterateAll()) {
            String day = row.get("Day").getStringValue();
            String topTerm = row.get("Top_Term").getStringValue();
            Long rank = row.get("rank").getLongValue();
            listResults.add(new Object[]{day, topTerm, rank});
        }
        Object[][] objects = listResults.stream().map(l -> Arrays.stream(l).map(Object::toString).toArray())
                .toArray(Object[][]::new);

        TextTable textTable = new TextTable(new String[] { "Day", "Top Term", "Rank" }, objects);
        textTable.printTable();
    }
}
