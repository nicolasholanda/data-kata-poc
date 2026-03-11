from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JAR_PATH = "/opt/spark-apps/data-kata-processing-1.0.0-SNAPSHOT.jar"

WAREHOUSE_ENV = {
    "WAREHOUSE_URL": "jdbc:postgresql://output-db:5432/warehousedb",
    "WAREHOUSE_USER": "warehouse_user",
    "WAREHOUSE_PASSWORD": "warehouse_pass",
}

SPARK_CONF = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
    "spark.openlineage.transport.type": "http",
    "spark.openlineage.transport.url": "http://marquez-api:5000",
    "spark.openlineage.namespace": "data-kata",
}

OPENLINEAGE_PACKAGES = "io.openlineage:openlineage-spark_2.13:1.44.0"

with DAG(
    dag_id="data_kata_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "start_date": "{{ (data_interval_start - macros.timedelta(days=30)).strftime('%Y-%m-%d') }}",
        "end_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
        "top_n": "10",
    },
) as dag:

    load_warehouse = SparkSubmitOperator(
        task_id="load_warehouse",
        application=JAR_PATH,
        java_class="com.github.nicolasholanda.processing.job.LoadWarehouseJob",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=OPENLINEAGE_PACKAGES,
        env_vars={
            **WAREHOUSE_ENV,
            "SOURCE_DB_URL": "jdbc:postgresql://source-db:5432/salesdb",
            "SOURCE_DB_USER": "sales_user",
            "SOURCE_DB_PASSWORD": "sales_pass",
            "CITIES_FILE_PATH": "/data/files/cities.csv",
            "PRODUCTS_FILE_PATH": "/data/files/products.csv",
            "SOAP_SERVICE_URL": "http://soap-mock:8090/salesmanservice",
            "SOAP_NAMESPACE": "http://soapmock.nicolasholanda.github.com/",
            "SOAP_SERVICE_NAME": "SalesmanService",
            "SOAP_PORT_NAME": "SalesmanServicePort",
        },
    )

    top_sales_by_city = SparkSubmitOperator(
        task_id="top_sales_by_city",
        application=JAR_PATH,
        java_class="com.github.nicolasholanda.processing.job.TopSalesByCityJob",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=OPENLINEAGE_PACKAGES,
        env_vars={
            **WAREHOUSE_ENV,
            "START_DATE": "{{ params.start_date }}",
            "END_DATE": "{{ params.end_date }}",
            "TOP_N": "{{ params.top_n }}",
        },
    )

    top_salesman = SparkSubmitOperator(
        task_id="top_salesman",
        application=JAR_PATH,
        java_class="com.github.nicolasholanda.processing.job.TopSalesmanJob",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=OPENLINEAGE_PACKAGES,
        env_vars={
            **WAREHOUSE_ENV,
            "START_DATE": "{{ params.start_date }}",
            "END_DATE": "{{ params.end_date }}",
            "TOP_N": "{{ params.top_n }}",
        },
    )

    load_warehouse >> [top_sales_by_city, top_salesman]
