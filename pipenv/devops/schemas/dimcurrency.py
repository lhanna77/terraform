# schemas.py
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

dim_currency_schema = StructType([
    StructField("CurrencyKey", IntegerType()),
    StructField("CurrencyAlternateKey", StringType()),
    StructField("CurrencyName", StringType())
])