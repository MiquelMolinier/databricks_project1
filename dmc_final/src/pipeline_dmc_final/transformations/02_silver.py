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
    name=f"{CATALOG}.{SILVER_SCHEMA}.clientes",
    comment="Clientes validados",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
@dp.expect_or_drop("pattern_email", r"email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z]+\.com$'")
@dp.expect_or_drop("category_segmento", "segmento IN ('Retail', 'Premium')")
def silver_clientes():
    return spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.clientes_raw").select(
        F.col("customer_id").cast("int").alias("customer_id"),
        F.trim("nombre").alias("nombre"),
        F.trim("apellido").alias("apellido"),
        F.lower(F.trim("email")).alias("email"),
        F.trim("ciudad").alias("ciudad"),
        F.trim("pais").alias("pais"),
        F.to_date("fecha_registro", "yyyy-MM-dd").alias("fecha_registro"),
        F.trim("segmento").alias("segmento"),
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.productos",
    comment="Productos validados",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("product_id_not_null", "product_id IS NOT NULL")
def silver_productos():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.productos_raw")
        .filter(F.col("_rescued_data").isNull())
        .select(
            F.col("product_id").cast("int").alias("product_id"),
            F.trim("nombre_producto").alias("nombre_producto"),
            F.trim("categoria").alias("categoria"),
            F.trim("subcategoria").alias("subcategoria"),
            F.col("precio_unitario").cast("decimal(10,2)").alias("precio_unitario"),
            F.trim("proveedor").alias("proveedor"),
            F.col("stock_actual").cast("int").alias("stock_actual"),
        )
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.pedidos",
    comment="Pedidos validados",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_fail("customer_id_not_null", "customer_id IS NOT NULL")
@dp.expect_or_drop(
    "valid_estado_pedido", "estado_pedido IN ('completado','en_proceso','cancelado')"
)
@dp.expect_or_drop(
    "valid_fecha_pedido", "to_date(fecha_pedido, 'yyyy-MM-dd') IS NOT NULL"
)
def silver_pedidos():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.pedidos_raw")
        .filter(F.col("_rescued_data").isNull())
        .select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("customer_id").cast("int").alias("customer_id"),
            F.to_date("fecha_pedido", "yyyy-MM-dd").alias("fecha_pedido"),
            F.trim("canal_venta").alias("canal_venta"),
            F.trim("estado_pedido").alias("estado_pedido"),
            F.col("total_pedido").cast("decimal(10,2)").alias("total_pedido"),
        )
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.detalle_pedidos",
    comment="Detalle de pedidos validado",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("order_item_id_not_null", "order_item_id IS NOT NULL")
@dp.expect_or_fail("order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_fail("product_id_not_null", "product_id IS NOT NULL")
def silver_detalle_pedidos():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.detalle_pedidos_raw")
        .filter(F.col("_rescued_data").isNull())
        .select(
            F.col("order_item_id").cast("int").alias("order_item_id"),
            F.col("order_id").cast("int").alias("order_id"),
            F.col("product_id").cast("int").alias("product_id"),
            F.col("cantidad").cast("int").alias("cantidad"),
            F.col("precio_unitario").cast("decimal(10,2)").alias("precio_unitario"),
            F.col("descuento").cast("decimal(5,2)").alias("descuento"),
        )
    )
