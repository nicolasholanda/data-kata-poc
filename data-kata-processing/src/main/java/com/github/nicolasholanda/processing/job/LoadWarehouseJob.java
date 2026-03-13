package com.github.nicolasholanda.processing.job;

import com.github.nicolasholanda.common.Tables;
import com.github.nicolasholanda.ingestion.DataSources;
import com.github.nicolasholanda.processing.SparkSessionFactory;
import com.github.nicolasholanda.processing.warehouse.WarehouseConfig;
import com.github.nicolasholanda.processing.warehouse.WarehouseReader;
import com.github.nicolasholanda.processing.warehouse.WarehouseWriter;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.utils.UUIDUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URI;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static com.github.nicolasholanda.common.Columns.*;

public class LoadWarehouseJob {

    public static void run(DataSources sources, WarehouseWriter writer, WarehouseReader reader) {
        writer.upsert(sources.cities(), Tables.DIM_CITY,
            new String[]{"id"}, new String[]{"name", "state"});
        writer.upsert(sources.salesmen(), Tables.DIM_SALESMAN,
            new String[]{"id"}, new String[]{"name"});
        writer.upsert(sources.products(), Tables.DIM_PRODUCT,
            new String[]{"id"}, new String[]{"name", "category", "base_price"});

        long watermark = reader.maxLong(Tables.FACT_SALES, "id");
        Dataset<Row> newSaleItems = sources.saleItems(watermark);

        if (newSaleItems.isEmpty()) {
            return;
        }

        Dataset<Row> sales = sources.sales();
        writer.append(buildFactSales(newSaleItems, sales), Tables.FACT_SALES);
    }

    private static Dataset<Row> buildFactSales(Dataset<Row> saleItems, Dataset<Row> sales) {
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
        run(new DataSources(spark), new WarehouseWriter(config), new WarehouseReader(spark, config));
        emitSoapLineage(spark, config);
    }

    private static void emitSoapLineage(SparkSession spark, WarehouseConfig config) {
        String marquezUrl = spark.conf().get("spark.openlineage.transport.url");
        String namespace = spark.conf().get("spark.openlineage.namespace");

        URI jdbcUri = URI.create(config.url().replace("jdbc:", ""));
        String outputNamespace = "postgres://" + jdbcUri.getHost() + ":" + jdbcUri.getPort();
        String database = jdbcUri.getPath().substring(1);

        try (OpenLineageClient client = OpenLineageClient.builder()
                .transport(HttpTransport.builder().uri(marquezUrl).build())
                .build()) {

            OpenLineage openLineage = new OpenLineage(URI.create("data-kata"));
            ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));

            InputDataset input = openLineage.newInputDatasetBuilder()
                .namespace("soap")
                .name("soap-mock/salesman-service")
                .build();

            OutputDataset output = openLineage.newOutputDatasetBuilder()
                .namespace(outputNamespace)
                .name(database + "." + Tables.DIM_SALESMAN)
                .build();

            OpenLineage.RunEvent event = openLineage.newRunEventBuilder()
                .eventType(EventType.COMPLETE)
                .eventTime(now)
                .run(openLineage.newRunBuilder().runId(UUIDUtils.generateNewUUID()).build())
                .job(openLineage.newJobBuilder().namespace(namespace).name("data_kata.fetch_salesmen").build())
                .inputs(List.of(input))
                .outputs(List.of(output))
                .build();

            client.emit(event);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
