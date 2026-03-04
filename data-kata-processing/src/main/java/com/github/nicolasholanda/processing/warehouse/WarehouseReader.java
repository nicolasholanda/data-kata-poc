package com.github.nicolasholanda.processing.warehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WarehouseReader {

    private final SparkSession spark;
    private final WarehouseConfig config;

    public WarehouseReader(SparkSession spark, WarehouseConfig config) {
        this.spark = spark;
        this.config = config;
    }

    public Dataset<Row> read(String table) {
        return spark.read()
            .format("jdbc")
            .option("url", config.url())
            .option("dbtable", table)
            .option("user", config.user())
            .option("password", config.password())
            .option("driver", "org.postgresql.Driver")
            .load();
    }
}
