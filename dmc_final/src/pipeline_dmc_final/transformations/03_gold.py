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


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_cliente", comment="Dimension de clientes"
)
def dim_cliente():
    clientes = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.clientes")

    window = Window.partitionBy("customer_id").orderBy(F.col("fecha_registro").desc())

    return (
        clientes.withColumn("rn", F.row_number().over(window))
        .filter("rn = 1")
        .drop("rn")
        .select(
            "customer_id",
            "nombre",
            "apellido",
            "email",
            "ciudad",
            "pais",
            "fecha_registro",
            "segmento",
        )
    )

@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_fecha", comment="Dimension calendario"
)
def dim_fecha():
    return (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.lit("2000-01-01").cast("date"),
                    F.greatest(F.lit("2050-12-31").cast("date"), F.current_date()),
                    F.expr("INTERVAL 1 DAY"),
                )
            ).alias("fecha")
        )
        .select(
            F.date_format("fecha", "yyyyMMdd").cast("int").alias("date_id"),
            F.col("fecha"),
            F.year("fecha").alias("anio"),
            F.quarter("fecha").alias("trimestre"),
            F.month("fecha").alias("mes"),
            F.dayofmonth("fecha").alias("dia"),
            F.weekofyear("fecha").alias("semana"),
            F.date_format("fecha", "EEEE").alias("nombre_dia"),
            F.dayofweek("fecha").alias("dia_semana"),
            F.dayofyear("fecha").alias("dia_anio"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_producto", comment="Dimension de productos"
)
@dp.expect_or_drop("precio_unitario_positive", "precio_unitario > 0")
@dp.expect_or_drop("stock_actual_non_negative", "stock_actual >= 0")
def dim_producto():
    return (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.productos")
        .select(
            "product_id",
            "nombre_producto",
            "categoria",
            "subcategoria",
            "precio_unitario",
            "proveedor",
            "stock_actual",
        )
        .dropDuplicates(["product_id"])
    )


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.fact_ventas",
    comment="Tabla de hechos de pedidos",
)
@dp.expect_or_drop("cantidad_positive", "cantidad > 0")
@dp.expect_or_drop("precio_unitario_positive", "precio_unitario > 0")
@dp.expect_or_drop("valid_descuento", "descuento BETWEEN 0 AND 1")
@dp.expect_or_drop("total_pedido_non_negative", "total_pedido >= 0")
@dp.expect_or_drop("valid_customer_id", "customer_key IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_key IS NOT NULL")
def fact_ventas():
    detalle = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.detalle_pedidos")

    pedidos = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.pedidos").select(
        "order_id",
        "customer_id",
        "fecha_pedido",
        "canal_venta",
        "estado_pedido",
        "total_pedido",
    )

    clientes = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.clientes")
        .select(F.col("customer_id").alias("customer_key"))
        .dropDuplicates()
    )

    productos = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.productos")
        .select(F.col("product_id").alias("product_key"))
        .dropDuplicates()
    )

    return (
        detalle.join(pedidos, "order_id", "inner")
        .join(clientes, F.col("customer_id") == F.col("customer_key"), "left")
        .join(productos, F.col("product_id") == F.col("product_key"), "left")
        .select(
            "order_item_id",
            "order_id",
            "customer_key",
            "product_key",
            "fecha_pedido",
            F.date_format("fecha_pedido", "yyyyMMdd").cast("int").alias("date_key"),
            "canal_venta",
            "estado_pedido",
            "total_pedido",
            "cantidad",
            "precio_unitario",
            "descuento",
        )
    )