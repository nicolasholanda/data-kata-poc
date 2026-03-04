package com.github.nicolasholanda.processing.warehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class WarehouseWriter {

    private final WarehouseConfig config;

    public WarehouseWriter(WarehouseConfig config) {
        this.config = config;
    }

    public void overwrite(Dataset<Row> dataset, String table) {
        write(dataset, table, SaveMode.Overwrite, true);
    }

    public void append(Dataset<Row> dataset, String table) {
        write(dataset, table, SaveMode.Append, false);
    }

    private void write(Dataset<Row> dataset, String table, SaveMode mode, boolean truncate) {
        dataset.write()
            .format("jdbc")
            .option("url", config.url())
            .option("dbtable", table)
            .option("user", config.user())
            .option("password", config.password())
            .option("driver", "org.postgresql.Driver")
            .option("truncate", truncate)
            .mode(mode)
            .save();
    }
}
