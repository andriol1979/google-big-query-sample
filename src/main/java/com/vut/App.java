package com.vut;

import com.vut.services.GoogleTrendQueryBuilder;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println("Hello World! This is the sample project to print Google Trend Big Query\n" +
                "This query shows a list of the daily top Google Search terms" );
        setEnv();
        runBigQuery();
    }

    private static void setEnv() {
        System.out.println("--- Print environment---");
        System.out.println(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
    }

    public static void runBigQuery() {
        GoogleTrendQueryBuilder googleTrendQueryBuilder = new GoogleTrendQueryBuilder();
        try {
            googleTrendQueryBuilder.buildQueryJob();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
