import sys
import os
from pyspark.sql import SparkSession
import pyspark

print("Starting Spark Configuration Diagnostic Test...")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
NESSIE_URI = os.getenv("NESSIE_URI")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

print(f"Target Nessie URI: {NESSIE_URI}")
print(f"Target AWS Region: {AWS_REGION}")

conf = (
        pyspark.SparkConf()
        .setAppName('app_name')
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0,software.amazon.awssdk:bundle:2.17.178')
        .set('spark.jars.ivy', '/home/docker/.ivy2')
        .set("spark.driver.userClassPathFirst", "true")
        .set("spark.executor.userClassPathFirst", "true")
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', NESSIE_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.warehouse', 's3a://warehouse')
        .set('spark.sql.catalog.nessie.s3.endpoint', 'http://minio:9000')
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set("spark.sql.catalog.nessie.cache-enabled", "false")
        .set("spark.driver.extraJavaOptions", "-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog")

        # S3FileIO reads THESE catalog-scoped keys, not fs.s3a.*
        .set('spark.sql.catalog.nessie.s3.access-key-id', MINIO_ACCESS_KEY)
        .set('spark.sql.catalog.nessie.s3.secret-access-key', MINIO_SECRET_KEY)
        .set('spark.sql.catalog.nessie.s3.path-style-access', 'true')  # needed for MinIO
        .set('spark.sql.catalog.nessie.s3.region', 'us-east-1') )

try:
    spark = SparkSession.builder.config(conf=conf).config("spark.sql.caseSensitive", "false").getOrCreate()
    print("✔ Spark Session created successfully.")

    print("Testing Catalog and S3 Connections...")
    spark.sql("SHOW NAMESPACES IN nessie").show()
    
    print("\n✅ Spark Config Test PASSED: All AWS/Iceberg/MinIO credentials & region settings are valid!")
    spark.stop()
    sys.exit(0)

except Exception as e:
    print(f"\n❌ Spark Config Test FAILED: {str(e)}")
    sys.exit(1)