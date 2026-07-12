from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DecimalType,
)
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("CATALOG")
LANDING_VOLUME = spark.conf.get("LANDING_VOLUME")
PROJECT_NAME = spark.conf.get("PROJECT_NAME")
LANDING_SCHEMA = spark.conf.get("LANDING_SCHEMA")
BRONZE_SCHEMA = spark.conf.get("BRONZE_SCHEMA")
SILVER_SCHEMA = spark.conf.get("SILVER_SCHEMA")
GOLD_SCHEMA = spark.conf.get("GOLD_SCHEMA")


@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.clientes_raw",
    comment="Ingesta cruda de clientes",
    table_properties={"quality": "bronze"},
)
def bronze_clients():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option(
            "cloudFiles.schemaLocation",
            f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/_schemas/{PROJECT_NAME}/clientes/",
        )
        .load(
            f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/{PROJECT_NAME}/clientes/"
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.productos_raw",
    comment="Ingesta cruda de productos",
    table_properties={"quality": "bronze"},
)
def bronze_products():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option(
            "cloudFiles.schemaLocation",
            f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/_schemas/{PROJECT_NAME}/productos/",
        )
        .load(
            f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/{PROJECT_NAME}/productos/"
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.pedidos_raw",
    comment="Ingesta cruda de pedidos",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option(
            "cloudFiles.schemaLocation",
            f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/_schemas/{PROJECT_NAME}/pedidos/",
        )
        .load(
            f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/{PROJECT_NAME}/pedidos/"
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.detalle_pedidos_raw",
    comment="Ingesta cruda de detalle de pedidos",
    table_properties={"quality": "bronze"},
)
def bronze_order_details():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option(
            "cloudFiles.schemaLocation",
            f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/_schemas/{PROJECT_NAME}/detalle_pedidos/",
        )
        .load(
            f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/{PROJECT_NAME}/detalle_pedidos/"
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
