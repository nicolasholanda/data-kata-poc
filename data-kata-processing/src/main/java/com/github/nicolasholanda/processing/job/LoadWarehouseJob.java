package com.github.nicolasholanda.processing.job;

import com.github.nicolasholanda.common.Tables;
import com.github.nicolasholanda.ingestion.DataSources;
import com.github.nicolasholanda.processing.SparkSessionFactory;
import com.github.nicolasholanda.processing.warehouse.WarehouseConfig;
import com.github.nicolasholanda.processing.warehouse.WarehouseWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.github.nicolasholanda.common.Columns.*;

public class LoadWarehouseJob {

    public static void run(DataSources sources, WarehouseWriter writer) {
        writer.overwrite(buildFactSales(sources), Tables.FACT_SALES);
        writer.overwrite(sources.cities(), Tables.DIM_CITY);
        writer.overwrite(sources.salesmen(), Tables.DIM_SALESMAN);
        writer.overwrite(sources.products(), Tables.DIM_PRODUCT);
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
        run(new DataSources(spark), new WarehouseWriter(WarehouseConfig.fromEnv()));
    }
}
