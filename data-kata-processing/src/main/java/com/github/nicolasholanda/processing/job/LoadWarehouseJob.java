package com.github.nicolasholanda.processing.job;

import com.github.nicolasholanda.common.Tables;
import com.github.nicolasholanda.ingestion.DataSources;
import com.github.nicolasholanda.processing.SparkSessionFactory;
import com.github.nicolasholanda.processing.warehouse.WarehouseConfig;
import com.github.nicolasholanda.processing.warehouse.WarehouseWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static com.github.nicolasholanda.common.Columns.*;

public class LoadWarehouseJob {

    public static void run(DataSources sources, WarehouseWriter writer, WarehouseConfig config) {
        truncateAll(config);
        writer.append(sources.cities(), Tables.DIM_CITY);
        writer.append(sources.salesmen(), Tables.DIM_SALESMAN);
        writer.append(sources.products(), Tables.DIM_PRODUCT);
        writer.append(buildFactSales(sources), Tables.FACT_SALES);
    }

    private static void truncateAll(WarehouseConfig config) {
        try (Connection conn = DriverManager.getConnection(config.url(), config.user(), config.password());
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE fact_sales, dim_city, dim_salesman, dim_product CASCADE");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Dataset<Row> buildFactSales(DataSources sources) {
        Dataset<Row> sales = sources.sales();
        Dataset<Row> saleItems = sources.saleItems();

        return saleItems
            .join(sales, saleItems.col(SaleItems.SALE_ID).equalTo(sales.col(Sales.ID)))
            .select(
                saleItems.col(SaleItems.ID),
                saleItems.col(SaleItems.SALE_ID),
                sales.col(Sales.SALESMAN_ID),
                sales.col(Sales.CITY_ID),
                saleItems.col(SaleItems.PRODUCT_ID),
                saleItems.col(SaleItems.QUANTITY),
                saleItems.col(SaleItems.UNIT_PRICE),
                saleItems.col(SaleItems.QUANTITY).multiply(saleItems.col(SaleItems.UNIT_PRICE)).alias(FactSales.LINE_TOTAL),
                sales.col(Sales.DATE)
            );
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create();
        WarehouseConfig config = WarehouseConfig.fromEnv();
        run(new DataSources(spark), new WarehouseWriter(config), config);
    }
}
