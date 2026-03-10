package com.github.nicolasholanda.processing;

import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    public static SparkSession create() {
        String appName = System.getenv().getOrDefault("SPARK_APP_NAME", "data-kata");
        String master = System.getenv("SPARK_MASTER");
        SparkSession.Builder builder = SparkSession.builder().appName(appName);
        if (master != null) {
            builder.master(master);
        }
        return builder.getOrCreate();
    }
}
