package com.github.nicolasholanda.ingestion.jdbc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SalesJdbcReader {

    private final SparkSession spark;
    private final JdbcConfig config;

    public SalesJdbcReader(SparkSession spark, JdbcConfig config) {
        this.spark = spark;
        this.config = config;
    }

    public Dataset<Row> readSales() {
        return read("sales");
    }

    public Dataset<Row> readSaleItems() {
        return read("sale_items");
    }

    private Dataset<Row> read(String table) {
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
