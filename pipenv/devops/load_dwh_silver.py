import os
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from schemas.dimdate import dim_date_schema
from schemas.dimcurrency import dim_currency_schema
from typing import Optional

# Enable Hive support and specify warehouse directory
spark = SparkSession.builder.getOrCreate()


def check_table_exists(db_name: str, table_name: str) -> bool:
    if spark.catalog.tableExists(f'{db_name}.{table_name}'):
        print(f"Table {db_name}.{table_name} exists")
        return True
    else:
        print(f"Table {db_name}.{table_name} does not exist")
        return False

def check_file_exists(csv_directory: str) -> bool:  
    if os.path.exists(f'{csv_directory}'):
        print(f'{csv_directory} exists')
        return True
    else:
        print(f'{csv_directory} does not exist')
        return False

def get_last_load_date(table_name: str) -> Optional[str]:
    
    if check_table_exists('ad_works',table_name) == True:
        
        # Add max CreatedDate to a variable
        max_created_date = spark.sql(f'SELECT MAX(CreatedDate) as max_date FROM ad_works.{table_name}').collect()[0]['max_date']
        
        print(max_created_date)

        return max_created_date
    
    else:
        return '2025-01-01'

def get_stage_load_count(table_name: str) -> int:
    
    if check_table_exists('stage_ad_works',table_name) == True:
        
        # Add max CreatedDate to a variable
        record_count = spark.sql(f'SELECT COUNT(*) as record_count FROM stage_ad_works.{table_name}').collect()[0]['record_count']
        
        print(f'record count - stage_ad_works.{table_name}: {record_count}')

        return record_count
    
    else:
        print('0 records in stage table')
        return 0
    

def create_ad_works_db_table(df_datalake, db_name: str, table_name: str, schema) -> bool:
    
    if check_table_exists(db_name,table_name) == False:
        # Create DATABASE ad_works if it does not exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        
        print(f'{db_name} database created')
        
        if db_name == 'ad_works':
            df_datalake.write.mode("overwrite").saveAsTable(f'{db_name}.{table_name}', schema=schema)
            spark.sql(f"TRUNCATE TABLE {db_name}.{table_name}")
            
            # Show table description
            spark.sql(f'DESCRIBE {db_name}.{table_name}').show()
            
            print(f'{db_name}.{table_name} table created')
        else:
            # Create table from df_datalake using schema dim_date_schema
            df_datalake.write.mode("overwrite").saveAsTable(f'{db_name}.{table_name}', schema=schema)
            print(f'{db_name}.{table_name} table created')
        
        return True
    
    else:
        print(f'{db_name}.{table_name} table already exists')
        return False

def create_artifact_csv(csv_file_name: str) -> str:
    
    # 1. Get the output directory from the environment variable
    artifact_output_dir = os.environ.get("BUILD_ARTIFACTSTAGINGDIRECTORY")

    # 2. Check if the environment variable is set (CRUCIAL)
    if artifact_output_dir is None:
        raise ValueError("BUILD_ARTIFACTSTAGINGDIRECTORY environment variable is not set.")

    # 3. Construct the full CSV file path
    csv_path = os.path.join(artifact_output_dir, csv_file_name)

    # 4. (Optional but Recommended) Create the directory if it doesn't exist 
    os.makedirs(artifact_output_dir, exist_ok=True)

    # 7. Return the CSV file path
    return csv_path

def create_csv_from_dwh(db_name: str, table_name: str, csv_directory: str) -> None:
    
    if check_table_exists(db_name, table_name):
        df_datalake = spark.sql(f'SELECT * FROM {db_name}.{table_name}')
        
        csv_artifact_directory = create_artifact_csv(f'{table_name}.csv')

        df_datalake.toPandas().to_csv(csv_directory, header=True, index=False)        
        df_datalake.toPandas().to_csv(csv_artifact_directory, header=True, index=False)
        print(f'{db_name}.{table_name} csv created')
    else:
        print(f'Table {db_name}.{table_name} does not exist')
  
def load_dwh_silver():

    # checks if a directory specified by `data_lake_path` exists. 
    data_lake_path = f'silver/'

    if not os.path.exists(data_lake_path):
        os.makedirs(data_lake_path)
        print(f'{data_lake_path} created')
    else:
        print(f'{data_lake_path} already exists')
        
    # Extract column names from the schema
    dim_date_column_names = [field.name for field in dim_date_schema.fields]
    dim_currency_column_names = [field.name for field in dim_currency_schema.fields]

    local = [{"table_name": "dimdate", "pk": "DateKey", "schema":dim_date_schema, "column_names": dim_date_column_names},
            {"table_name": "dimcurrency", "pk": "CurrencyKey", "schema":dim_currency_schema, "column_names": dim_currency_column_names}
            ]

    for table in local:
        
        # Define the directory containing the parquet files
        parquet_directory = f'data_lake/{table["table_name"]}'
        csv_directory = f'silver/{table["table_name"]}.csv'

        df_datalake = spark.read.parquet(parquet_directory)
        
        df_datalake_filtered = df_datalake.filter(df_datalake['CreatedDate'] > get_last_load_date(table["table_name"]))
        
        # Update df_datalake with column_names from local
        for old_col, new_col in zip(df_datalake_filtered.columns, table['column_names']):
            df_datalake_filtered = df_datalake_filtered.withColumnRenamed(old_col, new_col)

        create_ad_works_db_table(df_datalake_filtered,'stage_ad_works',table["table_name"],schema=["schema"])

        if get_stage_load_count(table["table_name"]) > 0:

            query = f'''
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY {table["pk"]} ORDER BY UpdatedDate DESC) as row_num
                FROM stage_ad_works.{table["table_name"]}
            ) WHERE row_num = 1
            '''
            
            df_datalake_dedupe = spark.sql(query)
            df_datalake_dedupe = df_datalake_dedupe.drop('row_num')
            
            df_datalake_empty = df_datalake_filtered.filter(df_datalake_filtered['CreatedDate'] < '1900-01-01')        
            
            create_ad_works_db_table(df_datalake_empty,'ad_works',table["table_name"],schema=["schema"])
            
        #     # Perform the merge operation
        #     # merge_query = f"""
        #     # MERGE INTO ad_works.{table["table_name"]} AS main
        #     # USING {table["table_name"]}_temp AS temp
        #     # ON main.DateKey = temp.DateKey
        #     # WHEN MATCHED THEN
        #     #     UPDATE SET main.UpdatedDate = temp.UpdatedDate
        #     # WHEN NOT MATCHED THEN
        #     #     INSERT *
        #     # """
        #     # spark.sql(merge_query)
            
        #     # MERGE INTO TABLE is not supported temporarily.

        # Count of new records to be inserted
        new_records_count = spark.sql(f"""
            SELECT COUNT(*) as count
            FROM stage_ad_works.{table["table_name"]}
            WHERE {table["pk"]} NOT IN (SELECT {table["pk"]} FROM ad_works.{table["table_name"]})
        """).collect()[0]['count']
        print(f'New records to be inserted: {new_records_count}')

        # Count of existing records to be updated
        existing_records_count = spark.sql(f"""
            SELECT COUNT(*) as count
            FROM stage_ad_works.{table["table_name"]} AS temp
            JOIN ad_works.{table["table_name"]} AS existing
            ON temp.{table["pk"]} = existing.{table["pk"]}
            WHERE temp.UpdatedDate > existing.UpdatedDate
        """).collect()[0]['count']
        print(f'Existing records to be updated: {existing_records_count}')  
        
        # Insert new records
        df_new_records = spark.sql(f"""
            SELECT temp.*
            FROM stage_ad_works.{table["table_name"]} AS temp
            LEFT JOIN ad_works.{table["table_name"]} AS existing
            ON temp.{table["pk"]} = existing.{table["pk"]}
            WHERE existing.{table["pk"]} IS NULL
        """)
        df_new_records.write.mode("append").saveAsTable(f'ad_works.{table["table_name"]}')
            
        create_csv_from_dwh('ad_works',table["table_name"],csv_directory)