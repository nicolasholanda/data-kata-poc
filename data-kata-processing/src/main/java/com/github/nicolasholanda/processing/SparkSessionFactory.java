package com.github.nicolasholanda.processing;

import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    public static SparkSession create() {
        String appName = System.getenv().getOrDefault("SPARK_APP_NAME", "data-kata");
        String master = System.getenv().getOrDefault("SPARK_MASTER", "local[*]");
        return SparkSession.builder()
            .appName(appName)
            .master(master)
            .getOrCreate();
    }
}
