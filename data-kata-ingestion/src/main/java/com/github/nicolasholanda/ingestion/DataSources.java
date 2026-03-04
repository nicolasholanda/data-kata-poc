package com.github.nicolasholanda.ingestion;

import com.github.nicolasholanda.ingestion.file.FileSourceConfig;
import com.github.nicolasholanda.ingestion.file.ReferenceDataFileReader;
import com.github.nicolasholanda.ingestion.jdbc.JdbcConfig;
import com.github.nicolasholanda.ingestion.jdbc.SalesJdbcReader;
import com.github.nicolasholanda.ingestion.soap.SalesmanSoapClient;
import com.github.nicolasholanda.ingestion.soap.SalesmanSoapConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSources {

    private final SalesJdbcReader jdbcReader;
    private final ReferenceDataFileReader fileReader;
    private final SalesmanSoapClient soapClient;

    public DataSources(SparkSession spark) {
        this.jdbcReader = new SalesJdbcReader(spark, JdbcConfig.fromEnv());
        this.fileReader = new ReferenceDataFileReader(spark, FileSourceConfig.fromEnv());
        this.soapClient = new SalesmanSoapClient(spark, SalesmanSoapConfig.fromEnv());
    }

    public Dataset<Row> sales() {
        return jdbcReader.readSales();
    }

    public Dataset<Row> saleItems() {
        return jdbcReader.readSaleItems();
    }

    public Dataset<Row> cities() {
        return fileReader.readCities();
    }

    public Dataset<Row> products() {
        return fileReader.readProducts();
    }

    public Dataset<Row> salesmen() {
        return soapClient.readSalesmen();
    }
}
