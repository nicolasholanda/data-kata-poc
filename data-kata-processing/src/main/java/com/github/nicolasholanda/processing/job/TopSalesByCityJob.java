package com.github.nicolasholanda.processing.job;

import com.github.nicolasholanda.common.Tables;
import com.github.nicolasholanda.processing.SparkSessionFactory;
import com.github.nicolasholanda.processing.warehouse.WarehouseConfig;
import com.github.nicolasholanda.processing.warehouse.WarehouseReader;
import com.github.nicolasholanda.processing.warehouse.WarehouseWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.time.LocalDate;

import static com.github.nicolasholanda.common.Columns.*;
import static org.apache.spark.sql.functions.*;

public class TopSalesByCityJob {

    private static final String TOTAL_SALES = "total_sales";
    private static final String CITY_NAME = "city_name";

    public static void run(WarehouseReader reader, WarehouseWriter writer,
                           LocalDate startDate, LocalDate endDate, int topN) {
        Dataset<Row> factSales = reader.read(Tables.FACT_SALES)
            .filter(col(FactSales.DATE).between(startDate.toString(), endDate.toString()));

        Dataset<Row> dimCity = reader.read(Tables.DIM_CITY);

        Dataset<Row> result = factSales
            .groupBy(FactSales.CITY_ID)
            .agg(sum(FactSales.LINE_TOTAL).alias(TOTAL_SALES))
            .join(dimCity, col(FactSales.CITY_ID).equalTo(dimCity.col(Cities.ID)))
            .select(dimCity.col(Cities.NAME).alias(CITY_NAME), dimCity.col(Cities.STATE), col(TOTAL_SALES))
            .orderBy(col(TOTAL_SALES).desc())
            .limit(topN)
            .withColumn(Results.PERIOD_START, lit(startDate.toString()).cast(DataTypes.DateType))
            .withColumn(Results.PERIOD_END, lit(endDate.toString()).cast(DataTypes.DateType))
            .withColumn(Results.COMPUTED_AT, current_timestamp());

        writer.append(result, Tables.TOP_SALES_BY_CITY);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create();
        WarehouseConfig config = WarehouseConfig.fromEnv();
        LocalDate startDate = LocalDate.parse(System.getenv("START_DATE"));
        LocalDate endDate = LocalDate.parse(System.getenv("END_DATE"));
        int topN = Integer.parseInt(System.getenv().getOrDefault("TOP_N", "10"));
        run(new WarehouseReader(spark, config), new WarehouseWriter(config), startDate, endDate, topN);
    }
}
