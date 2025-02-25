# schemas.py
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

dim_date_schema = StructType([
    StructField("DateKey", IntegerType()),
    StructField("FullDateAlternateKey", DateType()),
    StructField("DayNumberOfWeek", IntegerType()),
    StructField("EnglishDayNameOfWeek", StringType()),
    StructField("SpanishDayNameOfWeek", StringType()),
    StructField("FrenchDayNameOfWeek", StringType()),
    StructField("DayNumberOfMonth", IntegerType()),
    StructField("DayNumberOfYear", IntegerType()),
    StructField("WeekNumberOfYear", IntegerType()),
    StructField("EnglishMonthName", StringType()),
    StructField("SpanishMonthName", StringType()),
    StructField("FrenchMonthName", StringType()),
    StructField("MonthNumberOfYear", IntegerType()),
    StructField("CalendarQuarter", IntegerType()),
    StructField("CalendarYear", IntegerType()),
    StructField("CalendarSemester", IntegerType()),
    StructField("FiscalQuarter", IntegerType()),
    StructField("FiscalYear", IntegerType()),
    StructField("FiscalSemester", IntegerType()),
    StructField("CreatedDate", DateType()),
    StructField("UpdatedDate", DateType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("day", IntegerType())
])