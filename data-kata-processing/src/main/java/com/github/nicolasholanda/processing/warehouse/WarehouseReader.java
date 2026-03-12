package com.github.nicolasholanda.processing.warehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

    public long maxLong(String table, String column) {
        String sql = "SELECT COALESCE(MAX(" + column + "), 0) FROM " + table;
        try (Connection conn = DriverManager.getConnection(config.url(), config.user(), config.password());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
