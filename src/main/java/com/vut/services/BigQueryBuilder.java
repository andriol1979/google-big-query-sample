package com.vut.services;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

public class BigQueryBuilder {

    private static BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

    public static BigQuery getInstance() {
        return bigQuery;
    }
}
