from pyspark.sql import SparkSession

def spark_session(app):
    return (SparkSession.builder.appName(app)
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type","hadoop")
        .config("spark.sql.catalog.local.warehouse","s3a://warehouse/iceberg")
        .config("spark.hadoop.fs.s3a.endpoint","http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key","minio")
        .config("spark.hadoop.fs.s3a.secret.key","minio123")
        .config("spark.hadoop.fs.s3a.path.style.access","true")
        .getOrCreate())
