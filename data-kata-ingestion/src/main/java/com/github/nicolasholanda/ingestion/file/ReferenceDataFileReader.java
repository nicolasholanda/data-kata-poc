package com.github.nicolasholanda.ingestion.file;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReferenceDataFileReader {

    private final SparkSession spark;
    private final FileSourceConfig config;

    public ReferenceDataFileReader(SparkSession spark, FileSourceConfig config) {
        this.spark = spark;
        this.config = config;
    }

    public Dataset<Row> readCities() {
        return readCsv(config.citiesPath());
    }

    public Dataset<Row> readProducts() {
        return readCsv(config.productsPath());
    }

    private Dataset<Row> readCsv(String path) {
        return spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path);
    }
}
