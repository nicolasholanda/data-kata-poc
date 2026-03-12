package com.github.nicolasholanda.processing.warehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.stream.Collectors;

public class WarehouseWriter {

    private final WarehouseConfig config;

    public WarehouseWriter(WarehouseConfig config) {
        this.config = config;
    }

    public void append(Dataset<Row> dataset, String table) {
        write(dataset, table, SaveMode.Append);
    }

    public void upsert(Dataset<Row> dataset, String table, String[] keyColumns, String[] valueColumns) {
        String stagingTable = "stg_" + table;
        write(dataset, stagingTable, SaveMode.Overwrite);

        String keys = String.join(", ", keyColumns);
        String updates = Arrays.stream(valueColumns)
            .map(col -> col + " = EXCLUDED." + col)
            .collect(Collectors.joining(", "));

        String sql = "INSERT INTO " + table + " SELECT * FROM " + stagingTable
            + " ON CONFLICT (" + keys + ") DO UPDATE SET " + updates;

        try (Connection conn = DriverManager.getConnection(config.url(), config.user(), config.password());
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            stmt.execute("DROP TABLE IF EXISTS " + stagingTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void write(Dataset<Row> dataset, String table, SaveMode mode) {
        dataset.write()
            .format("jdbc")
            .option("url", config.url())
            .option("dbtable", table)
            .option("user", config.user())
            .option("password", config.password())
            .option("driver", "org.postgresql.Driver")
            .mode(mode)
            .save();
    }
}
