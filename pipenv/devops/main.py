from pyspark.sql import SparkSession
from load_data_lake import load_data_lake
from load_dwh_silver import load_dwh_silver

def create_spark_session(app_name="MySparkApp", warehouse_dir="/user/hive/warehouse"): # Default warehouse dir
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", warehouse_dir)  \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def stop_spark_session(spark):
    """Stops the SparkSession."""
    if spark:
        spark.stop()

if __name__ == "__main__":
    spark = create_spark_session()

    try:
        load_data_lake()
        load_dwh_silver()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        stop_spark_session(spark)