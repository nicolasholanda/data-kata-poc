from datetime import datetime

from airflow import DAG
from airflow.models import Connection, Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JAR_PATH = "/opt/spark-apps/data-kata-processing-1.0.0-SNAPSHOT.jar"
OPENLINEAGE_PACKAGES = "io.openlineage:openlineage-spark_2.13:1.44.0"

SPARK_CONF = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
    "spark.openlineage.transport.type": "http",
    "spark.openlineage.transport.url": "http://marquez-api:5000",
    "spark.openlineage.namespace": "data-kata",
}


def jdbc_url(conn):
    return f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"


def warehouse_env():
    conn = Connection.get_connection_from_secrets("warehouse_db")
    return {
        "WAREHOUSE_URL": jdbc_url(conn),
        "WAREHOUSE_USER": conn.login,
        "WAREHOUSE_PASSWORD": conn.password,
    }


def source_db_env():
    conn = Connection.get_connection_from_secrets("source_db")
    return {
        "SOURCE_DB_URL": jdbc_url(conn),
        "SOURCE_DB_USER": conn.login,
        "SOURCE_DB_PASSWORD": conn.password,
    }


def soap_env():
    conn = Connection.get_connection_from_secrets("soap_mock")
    extra = conn.extra_dejson
    return {
        "SOAP_SERVICE_URL": f"http://{conn.host}:{conn.port}/salesmanservice",
        "SOAP_NAMESPACE": extra["namespace"],
        "SOAP_SERVICE_NAME": extra["service_name"],
        "SOAP_PORT_NAME": extra["port_name"],
    }


def file_env():
    return {
        "CITIES_FILE_PATH": Variable.get("cities_file_path"),
        "PRODUCTS_FILE_PATH": Variable.get("products_file_path"),
    }


with DAG(
    dag_id="data_kata_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 0,
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
            **warehouse_env(),
            **source_db_env(),
            **file_env(),
            **soap_env(),
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
            **warehouse_env(),
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
            **warehouse_env(),
            "START_DATE": "{{ params.start_date }}",
            "END_DATE": "{{ params.end_date }}",
            "TOP_N": "{{ params.top_n }}",
        },
    )

    load_warehouse >> [top_sales_by_city, top_salesman]
